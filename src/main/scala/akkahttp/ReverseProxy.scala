package akkahttp

import io.circe._
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.Uri.Authority
import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.http.scaladsl.model.headers.{Host, RawHeader}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.pattern.CircuitBreaker
import org.apache.pekko.stream.scaladsl.Source
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future, Promise, TimeoutException}
import scala.util.{Failure, Success}

/**
  * Inspired by:
  * https://github.com/mathieuancelin/akka-http-reverse-proxy
  *
  * HTTP reverse proxy PoC with:
  *  - weighted round robin load balancing
  *  - retry on 5xx
  *
  * Setup:
  * HTTP client(s) --> ReverseProxy --> target server(s)
  *
  * Inspired by:
  * https://github.com/mathieuancelin/akka-http-reverse-proxy
  *
  * curl client:
  * curl -H "Host: local" -o - -i -w " %{time_total}\n" http://127.0.0.1:8080/mypath
  * curl -H "Host: external" -o - -i -w " %{time_total}\n" http://127.0.0.1:8080/200
  * wrk perf client:
  * wrk -t2 -c10 -d10s -H "Host: external" --latency http://127.0.0.1:8080/200
  */
object ReverseProxy extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  implicit val executionContext = system.dispatcher

  implicit val http = Http(system)

  val circuitBreakers = new ConcurrentHashMap[String, CircuitBreaker]()

  val proxyHost = "127.0.0.1"
  val proxyPort = 8080

  val targetHostLocal = "local"
  val targetHostExternal = "external"

  val counter = new AtomicInteger(0)

  // HTTP client(s)
  val clients = 1 to 2
  clients.par.foreach(clientID => httpClient(clientID, proxyHost, proxyPort, targetHostLocal, 10))

  def httpClient(clientId: Int, proxyHost: String, proxyPort: Int, targetHost: String, nbrOfRequests: Int) = {
    Source(1 to nbrOfRequests)
      .throttle(1, 1.seconds)
      .wireTap(each => logger.info(s"Client: $clientId about to send $each/$nbrOfRequests request..."))
      .mapAsync(1)(_ => http.singleRequest(HttpRequest(uri = s"http://$proxyHost:$proxyPort/").withHeaders(Seq(RawHeader("Host", targetHost)))))
      .wireTap(response => logger.info(s"Client: $clientId got response: ${response.status.intValue()}"))
      // https://nightlies.apache.org/pekko/docs/pekko-http/1.0/docs/implications-of-streaming-http-entity.html
      .runForeach(response => response.discardEntityBytes())
  }

  // ReverseProxy
  def NotFound(path: String) = HttpResponse(
    404,
    entity = HttpEntity(ContentTypes.`application/json`, Json.obj("error" -> Json.fromString(s"$path not found")).noSpaces)
  )

  def GatewayTimeout() = HttpResponse(
    504,
    entity = HttpEntity(ContentTypes.`application/json`, Json.obj("error" -> Json.fromString(s"Target servers timeout")).noSpaces)
  )

  def BadGateway(message: String) = HttpResponse(
    502,
    entity = HttpEntity(ContentTypes.`application/json`, Json.obj("error" -> Json.fromString(message)).noSpaces)
  )

  val services: Map[String, Seq[Target]] = Map(
    targetHostLocal -> Seq(
      Target.weighted("http://127.0.0.1:9081", 1),
      Target.weighted("http://127.0.0.1:9082", 2),
      Target.weighted("http://127.0.0.1:9083", 3)
    ),
    targetHostExternal -> Seq(
      Target.weighted("https://httpstat.us:443", 1),
      Target.weighted("https://httpstat.us:443", 2),
      Target.weighted("https://httpstat.us:443", 3)
    )
  )

  def extractHost(request: HttpRequest): String = request.header[Host].map(_.host.address()).getOrElse("--")

  def handlerWithCircuitBreaker(request: HttpRequest): Future[HttpResponse] = {
    val host = extractHost(request)
    services.get(host) match {
      case Some(rawSeq) =>
        val seq = rawSeq.flatMap(t => (1 to t.weight).map(_ => t))
        Retry.retry[HttpResponse](3) {
          val index = counter.incrementAndGet() % (if (seq.nonEmpty) seq.size else 1)
          val target = seq.apply(index)
          val circuitBreaker = circuitBreakers.computeIfAbsent(target.url, _ => new CircuitBreaker(
            system.scheduler,
            maxFailures = 5,
            callTimeout = 30.seconds,
            resetTimeout = 10.seconds))
          val headersIn: Seq[HttpHeader] =
            request.headers.filterNot(t => t.name() == "Host") :+
              Host(target.host) :+
              RawHeader("X-Fowarded-Host", host) :+
              RawHeader("X-Fowarded-Scheme", request.uri.scheme)
          val uri: Uri = request.uri.copy(
            scheme = target.scheme,
            authority = Authority(host = Uri.NamedHost(target.host), port = target.port))
          // Filter, to avoid log noise, see: https://github.com/akka/akka-http/issues/64
          val filteredHeaders = headersIn.toList.filterNot(each => each.name() == "Timeout-Access")
          val proxyReq = request.withUri(uri).withHeaders(filteredHeaders)
          circuitBreaker.withCircuitBreaker(http.singleRequest(proxyReq))
        }.recover {
          case _: akka.pattern.CircuitBreakerOpenException => BadGateway("Circuit breaker opened")
          case _: TimeoutException => GatewayTimeout()
          case e => BadGateway(e.getMessage)
        }
      case None => Future.successful(NotFound(host))
    }
  }

  val futReverseProxy = Http().newServerAt(proxyHost, proxyPort).bind(handlerWithCircuitBreaker)

  futReverseProxy.onComplete {
    case Success(b) =>
      logger.info("ReverseProxy started, listening on: " + b.localAddress)
    case Failure(e) =>
      logger.info(s"ReverseProxy failed. Exception message: ${e.getMessage}")
      system.terminate()
  }

  // Local target servers (with faulty behaviour)
  val echoRoute: Route =
    extractUri { uri =>
      complete {
        // Adjust to provoke more retries on ReverseProxy
        val codes = List(200, 200, 200, 500, 500, 500)
        val randomResponse = codes(new scala.util.Random().nextInt(codes.length))
        // TODO Why is the port 0?
        logger.info(s"Local target server listening on: ${uri.authority.port} got echo request, reply with: $randomResponse")
        StatusCode.int2StatusCode(randomResponse)
      }
    }

  services.get(targetHostLocal).foreach(targetSeq =>
    targetSeq.foreach(target => {
      val futTargetServer = Http().newServerAt(target.host, target.port).bind(echoRoute)

      futTargetServer.onComplete {
        case Success(b) =>
          logger.info(s"Target server started, listening on: ${b.localAddress}")
        case Failure(e) =>
          logger.info(s"Target server could not bind to... Exception message: ${e.getMessage}")
          system.terminate()
      }
    }
    )
  )

  // Models
  case class Target(scheme: String, host: String, port: Int, weight: Int = 1, protocol: HttpProtocol = HttpProtocols.`HTTP/1.1`) {
    def url: String = s"$scheme://$host:$port"
  }

  object Target {
    def apply(url: String): Target = {
      url.split("://|:").toList match {
        case scheme :: host :: port :: Nil => Target(scheme, host, port.toInt)
        case _ => throw new RuntimeException(s"Can not resolve target url: $url.")
      }
    }

    def weighted(url: String, weight: Int): Target = {
      Target(url).copy(weight = weight)
    }
  }
}

object Retry {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def retry[T](times: Int)(f: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val promise = Promise[T]()
    retryPromise[T](times, promise, None, f)
    promise.future
  }

  private[this] def retryPromise[T](times: Int, promise: Promise[T], failure: Option[Throwable],
                                    f: => Future[T])(implicit ec: ExecutionContext): Unit = {
    (times, failure) match {
      case (0, Some(e)) =>
        promise.tryFailure(e)
      case (0, None) =>
        promise.tryFailure(new RuntimeException("Failure, but lost track of exception :-("))
      case (_, _) =>
        f.onComplete {
          case Success(t) =>
            // we need to cast here
            val code = t.asInstanceOf[HttpResponse].status.intValue()
            if (code >= 500) {
              logger.info(s"Got 5xx server error from target server on attempt: $times/3")
              retryPromise[T](times - 1, promise, Some(new RuntimeException("Got 500")), f)
            } else {
              promise.trySuccess(t)
            }

          case Failure(e) =>
            retryPromise[T](times - 1, promise, Some(e), f)
        }(ec)
    }
  }
}
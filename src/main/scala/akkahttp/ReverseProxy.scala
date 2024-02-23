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
  *  - X-Correlation-ID (only for local)
  *
  * Setup local:
  * HTTP client(s) --> ReverseProxy --> local target server(s)
  *
  * Setup external:
  * HTTP client(s) --> ReverseProxy --> httpstat.us
  *
  * curl client:
  * curl -H "Host: local" -H "X-Correlation-ID: 1-1" -o - -i -w " %{time_total}\n" http://127.0.0.1:8080/mypath
  * curl -H "Host: external" -o - -i -w " %{time_total}\n" http://127.0.0.1:8080/200
  *
  * wrk perf client:
  * wrk -t2 -c10 -d10s -H "Host: external" --latency http://127.0.0.1:8080/200
  */
object ReverseProxy extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  implicit val executionContext = system.dispatcher

  implicit val http = Http(system)

  val circuitBreakers = new ConcurrentHashMap[String, CircuitBreaker]()
  val counter = new AtomicInteger(0)

  val proxyHost = "127.0.0.1"
  val proxyPort = 8080

  // HTTP client(s)
  val targetHostLocal = "local"
  val targetHostExternal = "external"

  val clients = 1 to 2
  clients.par.foreach(clientID => httpClient(clientID, proxyHost, proxyPort, targetHostLocal, 10))

  def httpClient(clientId: Int, proxyHost: String, proxyPort: Int, targetHost: String, nbrOfRequests: Int) = {
    def logResponse(response: HttpResponse) = {
      val id = if (response.getHeader("X-Correlation-ID").isPresent) {
        response.getHeader("X-Correlation-ID").get().value()
      } else {
        "N/A"
      }
      logger.info(s"Client: $clientId got response with id: $id and status: ${response.status.intValue()}")
    }

    Source(1 to nbrOfRequests)
      .throttle(1, 1.seconds)
      .wireTap(each => logger.info(s"Client: $clientId about to send request with id: $clientId-$each..."))
      .mapAsync(1)(each => http.singleRequest(HttpRequest(uri = s"http://$proxyHost:$proxyPort/").withHeaders(Seq(RawHeader("Host", targetHost), RawHeader("X-Correlation-ID", s"$clientId-$each")))))
      .wireTap(response => logResponse(response))
      // https://nightlies.apache.org/pekko/docs/pekko-http/1.0/docs/implications-of-streaming-http-entity.html
      .runForeach(response => response.discardEntityBytes())
  }

  // ReverseProxy
  def NotFound(id: String = "N/A", path: String) = HttpResponse(
    404,
    entity = HttpEntity(ContentTypes.`application/json`, Json.obj("error" -> Json.fromString(s"$path not found")).noSpaces)
  ).withHeaders(Seq(RawHeader("X-Correlation-ID", id)))

  def GatewayTimeout(id: String = "N/A") = HttpResponse(
    504,
    entity = HttpEntity(ContentTypes.`application/json`, Json.obj("error" -> Json.fromString(s"Target servers timeout")).noSpaces)
  ).withHeaders(Seq(RawHeader("X-Correlation-ID", id)))

  def BadGateway(id: String = "N/A", message: String) = HttpResponse(
    502,
    entity = HttpEntity(ContentTypes.`application/json`, Json.obj("error" -> Json.fromString(message)).noSpaces)
  ).withHeaders(Seq(RawHeader("X-Correlation-ID", id)))

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

  def handlerWithCircuitBreaker(request: HttpRequest): Future[HttpResponse] = {
    val host = request.header[Host].map(_.host.address()).getOrElse("N/A")
    val id = if (request.getHeader("X-Correlation-ID").isPresent) {
      request.getHeader("X-Correlation-ID").get().value()
    } else {
      "N/A"
    }

    def headers(target: Target) = {
      val headersIn: Seq[HttpHeader] =
        request.headers.filterNot(t => t.name() == "Host") :+
          Host(target.host, target.port) :+
          RawHeader("X-Fowarded-Host", host) :+
          RawHeader("X-Fowarded-Scheme", request.uri.scheme) :+
          RawHeader("X-Correlation-ID", id)

      // Filter, to avoid log noise, see: https://github.com/akka/akka-http/issues/64
      val filteredHeaders = headersIn.toList.filterNot(each => each.name() == "Timeout-Access")
      filteredHeaders
    }

    def uri(target: Target) = {
      val uri: Uri = request.uri.copy(
        scheme = target.scheme,
        authority = Authority(host = Uri.NamedHost(target.host), port = target.port))
      uri
    }

    services.get(host) match {
      case Some(rawSeq) =>
        val seq = rawSeq.flatMap(t => (1 to t.weight).map(_ => t))
        Retry.retry[HttpResponse](times = 3) {
          val index = counter.incrementAndGet() % (if (seq.isEmpty) 1 else seq.size)
          val target = seq.apply(index)
          val circuitBreaker = circuitBreakers.computeIfAbsent(target.url, _ => new CircuitBreaker(
            system.scheduler,
            // TODO 'maxFailures' vs 'times' above
            maxFailures = 5,
            callTimeout = 30.seconds,
            resetTimeout = 10.seconds))
          val proxyReq = request.withUri(uri(target)).withHeaders(headers(target))
          circuitBreaker.withCircuitBreaker(http.singleRequest(proxyReq))
        }.recover {
          case _: akka.pattern.CircuitBreakerOpenException => BadGateway(id, "Circuit breaker opened")
          case _: TimeoutException => GatewayTimeout(id)
          case e => BadGateway(id, e.getMessage)
        }
      case None => Future.successful(NotFound(id, host))
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
    extractRequest { request =>

      complete {
        val id = if (request.getHeader("X-Correlation-ID").isPresent) {
          request.getHeader("X-Correlation-ID").get().value()
        } else {
          "N/A"
        }

        // Adjust to provoke more retries on ReverseProxy
        val codes = List(200, 200, 200, 500, 500, 500)
        val randomResponse = codes(new scala.util.Random().nextInt(codes.length))
        logger.info(s"Target server listening on: ${request.uri.authority.host}:${request.uri.effectivePort} got echo request with id: $id, reply with: $randomResponse")
        (StatusCode.int2StatusCode(randomResponse), Seq(RawHeader("X-Correlation-ID", id)))
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
            val id = if (t.asInstanceOf[HttpResponse].getHeader("X-Correlation-ID").isPresent) {
              t.asInstanceOf[HttpResponse].getHeader("X-Correlation-ID").get().value()
            } else {
              "N/A"
            }
            if (code >= 500) {
              logger.info(s"Got 5xx server error from target server for id: $id. Retries left: ${times - 1}")
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
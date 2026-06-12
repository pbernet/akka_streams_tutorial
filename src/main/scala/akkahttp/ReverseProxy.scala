package akkahttp

import akkahttp.ReverseProxy.Mode.Mode
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.circe.*
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.Uri.Authority
import org.apache.pekko.http.scaladsl.model.headers.{Host, RawHeader}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.settings.ServerSettings
import org.apache.pekko.http.scaladsl.{Http, HttpExt}
import org.apache.pekko.pattern.{CircuitBreaker, CircuitBreakerOpenException}
import org.apache.pekko.stream.ThrottleMode
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import org.apache.pekko.util.ByteString
import org.bouncycastle.util.encoders.Hex
import org.slf4j.{Logger, LoggerFactory}

import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ThreadLocalRandom}
import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable
import scala.concurrent.*
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

/**
  * This conceptual all-in-one PoC is inspired by:
  * https://github.com/mathieuancelin/akka-http-reverse-proxy
  *
  * Features ReverseProxy:
  *  - Weighted round-robin load balancing
  *  - Retry on HTTP 5xx from target servers
  *  - CircuitBreaker per target server to avoid overload
  *  - HTTP Header `X-Correlation-ID` for tracing (only for Mode.local)
  *  - HTTP Header `X-Content-Hash` as an example of an on-the-fly processing scenario
  *  - Visualize traffic with [[ReverseProxyMonitor]]
  *
  * Mode.local (default):
  * HTTP client(s) --> ReverseProxy --> local target server(s)
  *
  * Mode.remote:
  * HTTP client(s) --> ReverseProxy --> remote target server(s)
  *
  * Remarks:
  *  - The target server selection is via the "Host" HTTP header
  *  - Local/Remote target servers are designed to be faulty to show Retry/CircuitBreaker behavior
  *    e.g. for mode Local adjust [[responseCodes]]
  *  - On top of the built-in client, you may also try other clients, see below
  *  - This PoC may not scale well, because the 'round robin' implementation
  *    with [[requestCounter]] means shared state
  *
  * Gatling client: [[ReverseProxySimulation]]
  *
  * curl client:
  * curl -H "Host: local" -H "X-Correlation-ID: 1-1" -o - -i -w " %{time_total}\n" http://127.0.0.1:8080/mypath
  * curl -H "Host: remote" -o - -i -w " %{time_total}\n" http://127.0.0.1:8080/status/200
  *
  * wrk perf clients:
  * wrk -t1 -c5 -d10s -H "Host: local" -H "X-Correlation-ID: wrk-t1" --latency http://127.0.0.1:8080/mypath &
  * wrk -t1 -c5 -d10s -H "Host: local" -H "X-Correlation-ID: wrk-t2" --latency http://127.0.0.1:8080/mypath &
  *
  * Without Correlation-ID:
  * wrk -t2 -c10 -d10s -H "Host: remote" --latency http://127.0.0.1:8080/status/200
  *
  * Doc:
  * https://pekko.apache.org/docs/pekko/current/common/circuitbreaker.html
  * https://pekko.apache.org/docs/pekko-http/current//implications-of-streaming-http-entity.html
  * https://pekko.apache.org/docs/pekko-http/current///common/timeouts.html#request-timeout
  */
object ReverseProxy extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val http: HttpExt = Http(system)

  val circuitBreakers = new ConcurrentHashMap[String, CircuitBreaker]()
  val requestCounter = new AtomicInteger(0)

  val proxyHost = "127.0.0.1"
  val proxyPort = 8080

  val services: Map[Mode, Seq[Target]] = Map(
    Mode.local -> Seq(
      Target.weighted("http://127.0.0.1:9081", 1),
      Target.weighted("http://127.0.0.1:9082", 2),
      Target.weighted("http://127.0.0.1:9083", 3)
    ),
    Mode.remote -> Seq(
      Target.weighted("https://httpbin.org:443", 1),
      Target.weighted("https://httpbin.org:443", 2),
      Target.weighted("https://httpbin.org:443", 3)
    )
  )

  // For Mode.local: Add more failure response codes to provoke more retries on ReverseProxy
  // and thus provoke the CircuitBreaker to open
  //val responseCodes = List(200, 200, 200, 200, 200, 200, 200, 200, 500, 503)
  val responseCodes = List(200, 200, 500, 500, 500, 500, 503, 503, 503, 503)

  localTargetServers(maxConnections = 100) // 1-1024
  reverseProxy()

  // Switch mode to let ReverseProxy forward client requests to local/remote target server(s)
  // Note that the remote servers can not interpret the X-Correlation-ID header
  val mode = Mode.local
  clients(nbrOfClients = 10, requestsPerClient = 100, mode)
  ReverseProxyMonitor.initializeWebUI(system, services(mode))

  sys.addShutdownHook {
    ReverseProxyMonitor.shutdown()
    system.terminate()
  }

  // HTTP client(s)
  def clients(nbrOfClients: Int = 1, requestsPerClient: Int = 1, mode: Mode): Unit = {
    logger.info(s"ReverseProxy: Running $nbrOfClients client(s), each sending $requestsPerClient requests")
    val clients = 1 to nbrOfClients
    clients.par.foreach(clientID => httpClient(clientID, proxyHost, proxyPort, mode, requestsPerClient))

    def httpClient(clientId: Int, proxyHost: String, proxyPort: Int, targetHost: Mode, nbrOfRequests: Int) = {
      def logResponse(response: HttpResponse): Unit = {
        val id = response.getHeader("X-Correlation-ID").orElse(RawHeader("X-Correlation-ID", "N/A")).value()
        val msg = response.entity.dataBytes.runReduce(_ ++ _).map(data => data.utf8String)
        msg.onComplete(msg => logger.info(s"[$id] Client: $clientId got response: ${response.status.intValue()} with msg: ${msg.getOrElse("N/A")}"))
      }

      val fixedPath = mode match {
        case Mode.local => ""
        case Mode.remote => "status/200,201,501,502,503,504"
      }

      Source(1 to nbrOfRequests)
        .throttle(1, 2.seconds, 10, ThrottleMode.shaping)
        .wireTap(each => logger.info(s"[$clientId-$each] Client: $clientId about to send request..."))
        .mapAsync(1)(each => http.singleRequest(HttpRequest(uri = s"http://$proxyHost:$proxyPort/$fixedPath")
          .withHeaders(Seq(RawHeader("Host", targetHost.toString), RawHeader("X-Correlation-ID", s"$clientId-$each")))))
        .wireTap(response => logResponse(response))
        .runWith(Sink.ignore)
    }
  }

  // ReverseProxy server
  def reverseProxy(): Unit = {
    def errorResponse(status: StatusCode, id: String, message: String): HttpResponse = HttpResponse(
      status,
      entity = HttpEntity(ContentTypes.`application/json`, Json.obj("error" -> Json.fromString(message)).noSpaces)
    ).withHeaders(RawHeader("X-Correlation-ID", id))

    def handlerWithCircuitBreaker(request: HttpRequest): Future[HttpResponse] = {
      val host = request.header[Host].map(_.host.address()).getOrElse("N/A")
      val mode = Mode.values.find(_.toString == host).getOrElse(Mode.local)
      val id = request.getHeader("X-Correlation-ID").orElse(RawHeader("X-Correlation-ID", "N/A")).value()

      val startTime = System.currentTimeMillis()

      def errorResponseFor(e: Throwable): HttpResponse = e match {
        case e: CircuitBreakerOpenException => errorResponse(StatusCodes.BadGateway, id, e.getMessage)
        case _: TimeoutException => errorResponse(StatusCodes.GatewayTimeout, id, "Target server timeout")
        case e => errorResponse(StatusCodes.BadGateway, id, e.getMessage)
      }

      def headers(target: Target): Seq[HttpHeader] =
        (request.headers.filterNot(_.name() == "Host") :+
          Host(target.host, target.port) :+
          RawHeader("X-Forwarded-Host", host) :+
          RawHeader("X-Forwarded-Scheme", request.uri.scheme) :+
          RawHeader("X-Correlation-ID", id))
          // Filter Timeout-Access to avoid log noise, see: https://github.com/akka/akka-http/issues/64
          .filterNot(_.name() == "Timeout-Access")

      def uri(target: Target): Uri =
        request.uri.copy(
          scheme = target.scheme,
          authority = Authority(Uri.NamedHost(target.host), target.port))

      def contentHashHeader: Flow[ByteString, RawHeader, NotUsed] =
        Flow[ByteString]
          .fold(MessageDigest.getInstance("SHA-256")) { (digest, chunk) =>
            digest.update(chunk.toArray)
            digest
          }
          .map(digest => RawHeader("X-Content-Hash", Hex.toHexString(digest.digest())))


      services.get(mode) match {
        case Some(rawSeq) =>
          val seq = rawSeq.flatMap(t => (1 to t.weight).map(_ => t))
          val index = requestCounter.incrementAndGet() % (if (seq.isEmpty) 1 else seq.size)
          val target = seq(index)
          logger.info(s"[$id] ReverseProxy: Forwarding request to $mode target server: ${target.url}")

          val requestId = ReverseProxyMonitor.logRequest(request, target.url, id)

          val circuitBreaker = circuitBreakers.computeIfAbsent(target.url, _ => {
            val cb = new CircuitBreaker(
              system.scheduler,
              maxFailures = 5,
              // Needs to be shorter than pekko-http 'request-timeout' (20s)
              // If not, clients get 503 from pekko-http
              callTimeout = 5.seconds,
              resetTimeout = 5.seconds)
            cb.onOpen(ReverseProxyMonitor.logCircuitBreakerEvent(target.url, "OPENED"))
            cb.onClose(ReverseProxyMonitor.logCircuitBreakerEvent(target.url, "CLOSED"))
            cb.onHalfOpen(ReverseProxyMonitor.logCircuitBreakerEvent(target.url, "HALF-OPENED"))
            cb
          })

          //  Example of an on-the-fly processing scenario
          val hashFuture = request.entity.dataBytes
            .via(contentHashHeader)
            .runWith(Sink.head)

          hashFuture.flatMap { hashHeader =>
            val proxyReq = request
              .withUri(uri(target))
              .withHeaders(headers(target) :+ hashHeader)

            // CircuitBreaker wraps the retry logic
            circuitBreaker.withCircuitBreaker {
              Retry.retry[HttpResponse](times = 3) {
                http.singleRequest(proxyReq)
              }
            }
          }.andThen {
            case Success(response) =>
              ReverseProxyMonitor.logResponse(requestId, response, System.currentTimeMillis() - startTime, id)
            case Failure(exception) =>
              ReverseProxyMonitor.logResponse(requestId, errorResponseFor(exception), System.currentTimeMillis() - startTime, id, Some(exception.getMessage))
          }.recover {
            case exception => errorResponseFor(exception)
          }
        case None =>
          val requestId = ReverseProxyMonitor.logRequest(request, host, id)
          val notFoundResponse = errorResponse(StatusCodes.NotFound, id, s"$host not found")
          ReverseProxyMonitor.logResponse(requestId, notFoundResponse, System.currentTimeMillis() - startTime, id, Some("Host not found"))
          Future.successful(notFoundResponse)
      }
    }
    val futReverseProxy = Http().newServerAt(proxyHost, proxyPort).bind(handlerWithCircuitBreaker)

    futReverseProxy.onComplete {
      case Success(b) =>
        logger.info(s"ReverseProxy: started, listening on: ${b.localAddress}")
      case Failure(e) =>
        logger.info(s"ReverseProxy: failed. Exception message: ${e.getMessage}")
        system.terminate()
    }
  }

  // Local target servers (with faulty behavior and throttled)
  def localTargetServers(maxConnections: Int): Unit = {
    val echoRoute: Route =
      extractRequest { request =>
        complete {
          Thread.sleep(ThreadLocalRandom.current.nextInt(1, 20) * 100)
          val id = request.getHeader("X-Correlation-ID").orElse(RawHeader("X-Correlation-ID", "N/A")).value()

          val randomResponseCode = responseCodes(new scala.util.Random().nextInt(responseCodes.length))
          logger.info(s"[$id] Target server: ${request.uri.authority.host}:${request.uri.effectivePort} got echo request, reply with: $randomResponseCode")
          (StatusCode.int2StatusCode(randomResponseCode), Seq(RawHeader("X-Correlation-ID", id)))
        }
      }

    services.get(Mode.local).foreach(targetSeq =>
      targetSeq.foreach(target => {

        // Tweaked config to throttle target servers
        val tweakedConf = ConfigFactory.empty()
          .withValue("pekko.http.server.max-connections", ConfigValueFactory.fromAnyRef(maxConnections))
          .withFallback(ConfigFactory.load())
        val serverSettings = ServerSettings(tweakedConf)
        val futTargetServer = Http().newServerAt(target.host, target.port)
          .withSettings(serverSettings)
          .bind(echoRoute)

        futTargetServer.onComplete {
          case Success(b) =>
            logger.info(s"Local target server: started, listening on: ${b.localAddress}")
          case Failure(e) =>
            logger.info(s"Local target server: could not bind to... Exception message: ${e.getMessage}")
            system.terminate()
        }
      }
      )
    )
  }

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

    // weight: 1-n, higher value means more requests will reach this server
    def weighted(url: String, weight: Int): Target = {
      Target(url).copy(weight = weight)
    }
  }

  object Mode extends Enumeration {
    type Mode = Value
    val local, remote = Value
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
      case (0, Some(e)) => promise.tryFailure(e)
      case (0, None) => promise.tryFailure(new RuntimeException("Failure, but lost track of exception"))
      case (_, _) =>
        f.onComplete {
          case Success(httpResponse: HttpResponse) if httpResponse.status.intValue() >= 500 =>
            val id = httpResponse.getHeader("X-Correlation-ID").orElse(RawHeader("X-Correlation-ID", "N/A")).value()
            logger.info(s"[$id] ReverseProxy: got 5xx server error. Retries left: ${times - 1}")
            val exception = new RuntimeException(s"Received: ${httpResponse.status.intValue()} from target server")
            retryPromise[T](times - 1, promise, Some(exception), f)
          case Success(t) => promise.trySuccess(t)
          case Failure(e) => retryPromise[T](times - 1, promise, Some(e), f)
        }
    }
  }
}
package akkahttp

import io.circe.generic.auto.*
import io.circe.syntax.*
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.scaladsl.{BroadcastHub, Flow, Sink, Source}
import org.apache.pekko.stream.{KillSwitches, SharedKillSwitch}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.Paths
import java.time.Instant
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters.*
import scala.sys.process.{Process, stringSeqToProcess}
import scala.util.{Failure, Success}

/**
  * Monitor for [[ReverseProxy]]
  *
  * Features:
  * - Real-time traffic monitoring via WebSockets
  * - HTML dashboard to show traffic data and
  * - Performance metrics (response times, error rates)
  */
object ReverseProxyMonitor {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  // Data models for monitoring
  private case class RequestInfo(
                                  id: String,
                                  timestamp: Long,
                                  method: String,
                                  uri: String,
                                  headers: Map[String, String],
                                  clientHost: String,
                                  targetUrl: String,
                                  correlationId: String
                                )

  private case class ResponseInfo(
                                   id: String,
                                   timestamp: Long,
                                   status: Int,
                                   headers: Map[String, String],
                                   responseTimeMs: Long,
                                   correlationId: String,
                                   errorMessage: Option[String] = None
                                 )

  private case class TrafficEntry(
                                   request: RequestInfo,
                                   response: Option[ResponseInfo] = None,
                                   duration: Option[Long] = None
                                 )

  private case class CircuitBreakerStatus(
                                           target: String,
                                           state: String,
                                           lastFailure: Option[Long]
                                         )

  private case class ProxyStats(
                                 totalRequests: Long,
                                 totalResponses: Long,
                                 errorRate: Double,
                                 avgResponseTime: Double,
                                 circuitBreakers: List[CircuitBreakerStatus]
                               )

  // In-memory storage for recent traffic (in production, consider using a proper time-series DB)
  private val trafficHistory = new ConcurrentLinkedQueue[TrafficEntry]()
  private val maxHistorySize = 1000

  // Circuit breaker states storage
  private val circuitBreakerStates = new ConcurrentHashMap[String, CircuitBreakerStatus]()

  // WebSocket broadcast hub for real-time updates
  private var broadcastKillSwitch: SharedKillSwitch = _
  private var eventSource: Source[String, NotUsed] = _

  def initializeWebUI(system: ActorSystem, targets: Seq[ReverseProxy.Target], port: Int = 9000): Future[Http.ServerBinding] = {
    implicit val actorSystem: ActorSystem = system
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    val sharedKillSwitch = KillSwitches.shared("websocket-broadcast")
    val source = Source
      .tick(1.second, 1.second, ())
      .map(_ => getCurrentStats.asJson.noSpaces)
      .via(sharedKillSwitch.flow)
      .runWith(BroadcastHub.sink)

    broadcastKillSwitch = sharedKillSwitch
    eventSource = source

    val route = createRoutes()
    val binding = Http().newServerAt("localhost", port).bind(route)

    targets.foreach { target =>
      logCircuitBreakerEvent(target.url, "CLOSED")
    }

    binding.onComplete {
      case Success(b) =>
        logger.info(s"ReverseProxyMonitor dashboard started at http://localhost:${b.localAddress.getPort}")
      case Failure(e) =>
        logger.error(s"Failed to start ReverseProxyMonitor dashboard: ${e.getMessage}")
    }
    browserClient()
    binding
  }

  def browserClient() = {
    val os = System.getProperty("os.name").toLowerCase
    if (os == "mac os x") Process(s"open http://127.0.0.1:9000").!
    else if (os.startsWith("windows")) Seq("cmd", "/c", s"start http://127.0.0.1:9000").!
  }


  def logRequest(request: HttpRequest, targetUrl: String, correlationId: String): String = {
    val requestId = java.util.UUID.randomUUID().toString
    val requestInfo = RequestInfo(
      id = requestId,
      timestamp = Instant.now().toEpochMilli,
      method = request.method.value,
      uri = request.uri.toString(),
      headers = request.headers.map(h => h.name() -> h.value()).toMap,
      clientHost = request.attribute(AttributeKeys.remoteAddress)
        .map(_.toString).getOrElse("unknown"),
      targetUrl = targetUrl,
      correlationId = correlationId
    )

    val entry = TrafficEntry(requestInfo)
    addToHistory(entry)
    logger.debug(s"Logged request: $correlationId -> $targetUrl")
    requestId
  }

  def logResponse(requestId: String, response: HttpResponse, responseTimeMs: Long,
                  correlationId: String, errorMessage: Option[String] = None): Unit = {
    val responseInfo = ResponseInfo(
      id = requestId,
      timestamp = Instant.now().toEpochMilli,
      status = response.status.intValue(),
      headers = response.headers.map(h => h.name() -> h.value()).toMap,
      responseTimeMs = responseTimeMs,
      correlationId = correlationId,
      errorMessage = errorMessage
    )

    updateHistoryWithResponse(requestId, responseInfo, responseTimeMs)
    logger.debug(s"Logged response: $correlationId -> ${response.status.intValue()} (${responseTimeMs}ms)")
  }

  def logCircuitBreakerEvent(target: String, state: String): Unit = {
    val circuitBreakerStatus = CircuitBreakerStatus(
      target = target,
      state = state,
      lastFailure = if (state.equals("OPENED")) Some(Instant.now().toEpochMilli) else None
    )
    circuitBreakerStates.put(target, circuitBreakerStatus)
    logger.info(s"Circuit breaker for: $target changed to: $state")
  }


  private def addToHistory(entry: TrafficEntry): Unit = {
    trafficHistory.offer(entry)
    // Keep history size manageable
    while (trafficHistory.size() > maxHistorySize) {
      trafficHistory.poll()
    }
  }

  private def updateHistoryWithResponse(requestId: String, responseInfo: ResponseInfo, duration: Long): Unit = {
    trafficHistory.asScala.find(_.request.id == requestId) match {
      case Some(entry) =>
        trafficHistory.remove(entry)
        trafficHistory.offer(entry.copy(response = Some(responseInfo), duration = Some(duration)))
      case None =>
        logger.warn(s"Could not find request entry for response: $requestId")
    }
  }

  private def getCurrentStats: ProxyStats = {
    val entries = trafficHistory.asScala.toList
    val totalRequests = entries.size.toLong
    val responsesWithData = entries.filter(_.response.isDefined)
    val totalResponses = responsesWithData.size.toLong
    val errors = responsesWithData.count(_.response.exists(_.status >= 400))
    val errorRate = if (totalResponses > 0) errors.toDouble / totalResponses else 0.0
    val avgResponseTime = if (responsesWithData.nonEmpty) {
      responsesWithData.map(_.duration.getOrElse(0L)).sum.toDouble / responsesWithData.size
    } else 0.0

    ProxyStats(
      totalRequests = totalRequests,
      totalResponses = totalResponses,
      errorRate = errorRate,
      avgResponseTime = avgResponseTime,
      circuitBreakers = circuitBreakerStates.values().asScala.toList
    )


  }

  private def createRoutes(): Route = {
    import org.apache.pekko.http.scaladsl.model.ws.*

    pathPrefix("api") {
      path("traffic") {
        get {
          complete {
            val recent = trafficHistory.asScala.takeRight(100).toList
            recent.asJson.noSpaces
          }
        }
      } ~
        path("stats") {
          get {
            complete {
              getCurrentStats.asJson.noSpaces
            }
          }
        } ~
        path("ws") {
          handleWebSocketMessages(Flow.fromSinkAndSource(
            Sink.ignore,
            eventSource.map(TextMessage(_))
          ))
        }
    } ~
      pathSingleSlash {
        get {
          val static = "src/main/resources"
          val dashboardHtml = Paths.get(static, "dashboard.html").toFile
          getFromFile(dashboardHtml, ContentTypes.`text/html(UTF-8)`)
        }
      }
  }

  def shutdown(): Unit = {
    if (broadcastKillSwitch != null) {
      broadcastKillSwitch.shutdown()
    }
  }
}
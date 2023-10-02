package sample.stream_actor

import org.apache.pekko.actor.{Actor, ActorLogging, Props}
import org.apache.pekko.http.scaladsl.model.StatusCode
import sample.stream_actor.WindTurbineSimulator._

case class WindTurbineSimulatorException(id: String) extends RuntimeException

/**
  * WindTurbineSimulator starts the [[WebSocketClient]] and coordinates
  * issues during:
  *  - startup
  *  - running
  */
object WindTurbineSimulator {
  def props(id: String, endpoint: String) =
    Props(new WindTurbineSimulator(id, endpoint))

  final case object Upgraded
  final case object Connected
  final case object Terminated
  final case class ConnectionFailure(ex: Throwable)
  final case class FailedUpgrade(statusCode: StatusCode)
}

class WindTurbineSimulator(id: String, endpoint: String)
  extends Actor with ActorLogging {
  implicit private val system = context.system
  implicit private val executionContext = system.dispatcher

  val webSocketClient = WebSocketClient(id, endpoint, self)

  override def receive: Receive = startup //initial state

  private def startup:  Receive = {
    case Upgraded =>
      log.info(s"$id : WebSocket upgraded")
    case FailedUpgrade(statusCode) =>
      log.error(s"$id : Failed to upgrade WebSocket connection: $statusCode")
      throw WindTurbineSimulatorException(id)
    case ConnectionFailure(ex) =>
      log.error(s"$id : Failed to establish WebSocket connection: $ex")
      throw WindTurbineSimulatorException(id)
    case Connected =>
      log.info(s"$id : WebSocket connected")
      context.become(running)
  }

  private def running: Receive = {
    case Terminated =>
      log.error(s"$id : WebSocket connection terminated")
      throw WindTurbineSimulatorException(id)
    case ConnectionFailure(ex) =>
      log.error(s"$id : ConnectionFailure occurred: $ex")
      throw WindTurbineSimulatorException(id)
  }
}

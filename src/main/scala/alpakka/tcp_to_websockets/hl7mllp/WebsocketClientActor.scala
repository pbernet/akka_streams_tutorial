package alpakka.tcp_to_websockets.hl7mllp

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.http.scaladsl.model.StatusCode
import alpakka.tcp_to_websockets.hl7mllp.WebsocketClientActor._

import scala.concurrent.duration._


case class ConnectionException(id: String) extends RuntimeException

object WebsocketClientActor {
  def props(id: String, endpoint: String, websocketConnectionStatusActor: ActorRef) =
    Props(new WebsocketClientActor(id, endpoint, websocketConnectionStatusActor))

  final case object Upgraded
  final case object Connected
  final case object Terminated
  final case class ConnectionFailure(ex: Throwable)
  final case class FailedUpgrade(statusCode: StatusCode)
  final case class SendMessage(msg: String)

}

class WebsocketClientActor(id: String, endpoint: String, websocketConnectionStatusActor: ActorRef)
  extends Actor with ActorLogging {
  implicit private val system = context.system
  implicit private val executionContext = system.dispatcher

  val webSocketClient = WebSocketClient(id, endpoint, self)

  override def receive: Receive = startup //initial state

  private def startup: Receive = {
    case Upgraded =>
      log.info(s"Client$id: WebSocket upgraded")
    case FailedUpgrade(statusCode) =>
      log.error(s"Client$id: Failed to upgrade WebSocket connection: $statusCode")
      websocketConnectionStatusActor ! WebsocketConnectionStatus.Terminated
      throw ConnectionException(id)
    case ConnectionFailure(ex) =>
      log.error(s"Client $id: Failed to establish WebSocket connection: $ex")
      websocketConnectionStatusActor ! WebsocketConnectionStatus.Terminated
      throw ConnectionException(id)
    case Connected =>
      log.info(s"Client $id: WebSocket connected")
      websocketConnectionStatusActor ! WebsocketConnectionStatus.Connected
      context.become(running)
    case SendMessage(msg) =>
      log.warning(s"In state startup. Can not receive message: $msg. Resend after 2 seconds")
      system.scheduler.scheduleOnce(2.seconds, self, SendMessage(msg))
  }

  private def running: Receive = {
    case SendMessage(msg) =>
      log.info(s"About to send message to WebSocket: $msg")
      webSocketClient.sendToWebsocket(msg)
    case Terminated =>
      log.error(s"Client $id: WebSocket connection terminated")
      websocketConnectionStatusActor ! WebsocketConnectionStatus.Terminated
      throw ConnectionException(id)
    case ConnectionFailure(ex) =>
      log.error(s"Client $id: ConnectionFailure occurred: $ex")
      websocketConnectionStatusActor ! WebsocketConnectionStatus.Terminated
      throw ConnectionException(id)
  }

  override def postStop(): Unit = {
    websocketConnectionStatusActor ! WebsocketConnectionStatus.Terminated
  }
}

package alpakka.tcp_to_websockets.websockets

import alpakka.tcp_to_websockets.websockets.WebsocketClientActor._
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.pekko.actor.{Actor, ActorLogging, ActorRef, Props}
import org.apache.pekko.http.scaladsl.model.StatusCode

import scala.concurrent.duration._


case class ConnectionException(cause: String) extends RuntimeException

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
      log.error(s"Client$id: failed to upgrade WebSocket connection: $statusCode")
      websocketConnectionStatusActor ! WebsocketConnectionStatusActor.Terminated
      throw ConnectionException(statusCode.toString())
    case ConnectionFailure(ex) =>
      log.error(s"Client $id: failed to establish WebSocket connection: $ex")
      websocketConnectionStatusActor ! WebsocketConnectionStatusActor.Terminated
      throw ConnectionException(ExceptionUtils.getRootCause(ex).getMessage)
    case Connected =>
      log.info(s"Client $id: WebSocket connected")
      websocketConnectionStatusActor ! WebsocketConnectionStatusActor.Connected
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
      websocketConnectionStatusActor ! WebsocketConnectionStatusActor.Terminated
      throw ConnectionException(s"Client $id: WebSocket connection terminated")
    case ConnectionFailure(ex) =>
      log.error(s"Client $id: ConnectionFailure occurred: $ex")
      websocketConnectionStatusActor ! WebsocketConnectionStatusActor.Terminated
      throw ConnectionException(ExceptionUtils.getRootCause(ex).getMessage)
  }

  override def postStop(): Unit = {
    websocketConnectionStatusActor ! WebsocketConnectionStatusActor.Terminated
  }
}

package alpakka.tcp_to_websockets.websockets

import alpakka.tcp_to_websockets.websockets.WebsocketConnectionStatusActor.{Connected, ConnectionStatus, Terminated}
import org.apache.pekko.actor.{Actor, ActorLogging, Props}


object WebsocketConnectionStatusActor {
  def props(id: String, endpoint: String) =
    Props(new WebsocketConnectionStatusActor(id, endpoint))

  final case object Connected
  final case object Terminated
  final case object ConnectionStatus

}

class WebsocketConnectionStatusActor(id: String, endpoint: String)
  extends Actor with ActorLogging {
  implicit private val system = context.system
  implicit private val executionContext = system.dispatcher

  var isConnected = false

  override def receive: Receive = {
    case Connected =>
      isConnected = true
      log.info(s"Client $id: connected to: $endpoint")

    case Terminated =>
      isConnected = false
      log.info(s"Client $id: terminated from: $endpoint")

    case ConnectionStatus =>
      sender() ! isConnected
  }
}

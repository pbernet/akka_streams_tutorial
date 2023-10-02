package akkahttp

import akkahttp.WebsocketEchoActors.Protocol.{CloseConnection, OpenConnection, SignedMessage}
import org.apache.pekko.Done
import org.apache.pekko.actor.{Actor, ActorRef, Props}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import org.apache.pekko.stream.{CompletionStrategy, OverflowStrategy}

import java.util.UUID
import scala.concurrent.Future
import scala.util.{Failure, Success}


/**
  * Similar to [[WebsocketEcho]] but uses Actors on the server to wrap the `websocketEchoFlow`
  * Runs with n `browserClients` depending on the configured `maxClients` on the server
  *
  * Features:
  *  - Keep the `outSourceActorRef` connections in memory
  *  - Close connection from the server side if `maxClients` is reached
  *  - Close connection from the server side upon explicit client closing
  *
  * Inspired by:
  * https://stackoverflow.com/questions/41316173/akka-websocket-how-to-close-connection-by-server?rq=1
  * https://stackoverflow.com/questions/69380115/akka-how-to-close-a-websocket-connection-from-server
  * https://doc.akka.io/docs/akka/current/stream/operators/Source/actorRef.html?_ga#description
  */
object WebsocketEchoActors extends App with ClientCommon {

  val (address, port) = ("127.0.0.1", 6002)
  server(address, port)
  browserClient()
  //To observe the limiting behaviour add another browser client (or open 2nd tab in 1st browser)
  //browserClient()

  val maxClients = 1

  class ChatRef extends Actor {
    override def receive: Receive = withClients(Map.empty[UUID, ActorRef])

    def withClients(clients: Map[UUID, ActorRef]): Receive = {
      case SignedMessage(uuid, msg) => clients.collect {
        case (id, ar) if id == uuid => ar ! msg
      }
      case OpenConnection(ar, _) if clients.size == maxClients => ar ! Done
      case OpenConnection(ar, uuid) => context.become(withClients(clients.updated(uuid, ar)))
      case CloseConnection(uuid) =>
        logger.info(s"CloseConnection for: $uuid")
        context.become(withClients(clients - uuid))
    }
  }

  object Protocol {
    case class SignedMessage(uuid: UUID, msg: String)

    case class OpenConnection(actor: ActorRef, uuid: UUID)

    case class CloseConnection(uuid: UUID)
  }

  def server(address: String, port: Int) = {

    val chatRef = system.actorOf(Props[ChatRef]())

    def websocketEchoFlow: Flow[Message, Message, Any] =
      Flow[Message]
        .mapAsync(1) {
          case TextMessage.Strict(s) => Future.successful(s)
          case TextMessage.Streamed(s) => s.runFold("")(_ + _)
          case b: BinaryMessage => throw new Exception(s"Binary message: $b cannot be handled")
        }.via(chatActorFlow(UUID.randomUUID()))
        .map(TextMessage(_))


    def chatActorFlow(connectionId: UUID): Flow[String, String, Any] = {

      val actorSink = Sink.actorRef(chatRef,
        onCompleteMessage = Protocol.CloseConnection(connectionId),
        onFailureMessage = _ => Protocol.CloseConnection(connectionId))

      val inSink = Flow[String]
        .map(msg => Protocol.SignedMessage(connectionId, msg))
        .to(actorSink)

      val outSource = Source.actorRef(
        completionMatcher = {
          case Done =>
            logger.info(s"Exceeded number of maxClients: $maxClients. Closing this connection.")
            CompletionStrategy.immediately
        },
        failureMatcher = PartialFunction.empty,
        bufferSize = 100,
        overflowStrategy = OverflowStrategy.dropHead)
        .mapMaterializedValue {
          outSourceActorRef: ActorRef => {
            chatRef ! Protocol.OpenConnection(outSourceActorRef, connectionId)
          }
        }

      Flow.fromSinkAndSource(inSink, outSource)
    }

    val route: Route = {
      path("echo") {
        extractRequest { request =>
          logger.info(s"Got echo request from client: ${request.getHeader("User-Agent")}")
          handleWebSocketMessages(websocketEchoFlow)
        }
      }
    }

    val bindingFuture = Http().newServerAt(address, port).bindFlow(route)
    bindingFuture.onComplete {
      case Success(b) =>
        logger.info(s"Server started, listening on: ${b.localAddress}")
      case Failure(e) =>
        logger.info(s"Server could not bind to: $address:$port. Exception message: ${e.getMessage}")
        system.terminate()
    }
  }
}

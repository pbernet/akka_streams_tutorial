package akkahttp

import akka.Done
import akka.actor.{Actor, ActorRef, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{CompletionStrategy, OverflowStrategy}
import akkahttp.WebsocketEcho.handleWebSocketMessages
import akkahttp.WebsocketEchoActors.Protocol.{CloseConnection, OpenConnection, SignedMessage}

import java.util.UUID
import scala.concurrent.Future
import scala.util.{Failure, Success}


/**
  * Similar to [[WebsocketEcho]] but uses Actors to wrap the `websocketEchoFlow`
  *
  * Features:
  *  - Keep the `outSourceActorRef` connections in memory
  *  - Close connection from the server side if `maximumClients` is reached
  *  - Close connection from the server side upon explicit client closing
  *  - Runs with n `browserClients`
  *
  * Inspired by:
  * https://stackoverflow.com/questions/41316173/akka-websocket-how-to-close-connection-by-server?rq=1
  * https://stackoverflow.com/questions/69380115/akka-how-to-close-a-websocket-connection-from-server
  * https://doc.akka.io/docs/akka/current/stream/operators/Source/actorRef.html?_ga=2.202814354.2030543212.1636101852-933363116.1515256566#description
  *
  */
object WebsocketEchoActors extends App with ClientCommon {

  val (address, port) = ("127.0.0.1", 6002)
  server(address, port)
  browserClient()
  //browserClient()

  val maximumClients = 1

  class ChatRef extends Actor {
    override def receive: Receive = withClients(Map.empty[UUID, ActorRef])

    def withClients(clients: Map[UUID, ActorRef]): Receive = {
      case SignedMessage(uuid, msg) => clients.collect {
        case (id, ar) if id == uuid => ar ! msg
      }
      case OpenConnection(ar, _) if clients.size == maximumClients => ar ! Done
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
          case b: BinaryMessage => throw new Exception("Binary message cannot be handled")
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
            logger.info(s"Exceeded number of maximumClients: $maximumClients. Close this connection.")
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
        logger.info("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        logger.info(s"Server could not bind to $address:$port. Exception message: ${e.getMessage}")
        system.terminate()
    }
  }
}

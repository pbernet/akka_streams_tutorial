package akkahttp

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage, WebSocketRequest}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.WebSocketDirectives
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.github.andyglow.websocket._

import scala.concurrent.{Future, Promise}
import scala.language.postfixOps
import scala.util.{Failure, Success}

object WebsocketEcho extends WebSocketDirectives {
  implicit val system = ActorSystem("my-system")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  def main(args: Array[String]) {
    val (address, port) = ("127.0.0.1", 6000)
    server(address, port)
    for ( a <- 1 to 10) clientProprietary(address, port)
    for ( a <- 1 to 10) clientSingleWebSocketRequest(address, port)

  }

  private def server(address: String, port: Int) = {

    def echo: Flow[Message, Message, Any] =
      Flow[Message].mapConcat {
        case tm: TextMessage =>
          println(s"Server recieved: $tm")
          TextMessage(Source.single("Hello ") ++ tm.textStream ++ Source.single("!")) :: Nil
        case bm: BinaryMessage =>
          // ignore binary messages but drain content to avoid the stream being clogged
          bm.dataStream.runWith(Sink.ignore)
          Nil
      }

    val websocketRoute: Route =
      path("echo") {
        handleWebSocketMessages(echo)
      }

    val bindingFuture = Http().bindAndHandle(websocketRoute, address, port)
    bindingFuture.onComplete {
      case Success(b) =>
        println("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        println(s"Server could not bind to $address:$port. Exception message: ${e.getMessage}")
        system.terminate()
    }
  }

  private def clientProprietary(address: String, port: Int) = {

    // prepare ws-client and define a message callback handler
    val cli = WebsocketClient[String](s"ws://$address:$port/echo") {
      case str => println(s"Client recieved String: $str")
    }
    val ws = cli.open()
    ws ! "world one"
    ws ! "world two"
  }

  private def clientSingleWebSocketRequest(address: String, port: Int) = {

    val printSink: Sink[Message, Future[Done]] =
      Sink.foreach {
        case TextMessage.Strict(text)             => println(s"Client recieved Strict: $text")
        case TextMessage.Streamed(textStream)     => textStream.runFold("")(_ + _).onComplete(value => println(s"Client recieved Streamed: ${value.get}"))
        case BinaryMessage.Strict(binary)         => //do nothing
        case BinaryMessage.Streamed(binaryStream) => binaryStream.runWith(Sink.ignore)
      }

    val helloSource: Source[TextMessage.Strict, NotUsed] = Source(List(TextMessage("world one"), TextMessage("world two")))

    val flow: Flow[Message, Message, Promise[Option[Message]]] =
      Flow.fromSinkAndSourceMat(
        printSink,
        //see http://doc.akka.io/docs/akka-http/10.0.9/scala/http/client-side/websocket-support.html#half-closed-client-websockets
        helloSource.concatMat(Source.maybe[Message])(Keep.right))(Keep.right)

    val (upgradeResponse, closed) =
    Http().singleWebSocketRequest(WebSocketRequest(s"ws://$address:$port/echo"), flow)

    val connected = upgradeResponse.map { upgrade =>
      // just like a regular http request we can access response status which is available via upgrade.response.status
      // status code 101 (Switching Protocols) indicates that server support WebSockets
      if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
        Done
      } else {
        throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
      }
    }

    // in a real application you would not side effect here and handle errors more carefully
    connected.onComplete(println)
    closed.future.foreach(_ => println("closed"))
  }
}

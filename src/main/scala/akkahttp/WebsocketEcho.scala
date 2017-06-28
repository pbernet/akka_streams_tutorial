package akkahttp

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.WebSocketDirectives
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.github.andyglow.websocket._

import scala.language.postfixOps
import scala.util.{Failure, Success}

object WebsocketEcho extends WebSocketDirectives {

  def main(args: Array[String]) {
    val (address, port) = ("127.0.0.1", 6000)
    server(address, port)
    for ( a <- 1 to 10) client(address, port)
  }

  private def server(address: String, port: Int) = {
    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    def echo: Flow[Message, Message, Any] =
      Flow[Message].mapConcat {
        case tm: TextMessage =>
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

  private def client(address: String, port: Int) = {

    // prepare ws-client and define message handler
    val cli = WebsocketClient[String](s"ws://$address:$port/echo") {
      case str => println(s"<<| $str")
    }
    val ws = cli.open()
    ws ! "world"
  }
}

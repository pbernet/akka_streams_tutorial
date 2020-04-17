package alpakka.tcp_to_websockets.hl7mllp

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.WebSocketDirectives
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.language.postfixOps
import scala.sys.process.Process
import scala.util.{Failure, Success}


/**
  * Websocket echo server with client browser client for testing
  *
  */
object WebsocketServer extends App with WebSocketDirectives {
  implicit val system = ActorSystem("WebsocketServer")
  implicit val executionContext = system.dispatcher

  val (address, port) = ("127.0.0.1", 6002)
  server(address, port)
  browserClient()

  def server(address: String, port: Int) = {

    def echoFlow: Flow[Message, Message, Any] =
      Flow[Message].mapConcat {
        case tm: TextMessage =>
          println(s"Server received: $tm")
          TextMessage(Source.single("Hello ") ++ tm.textStream ++ Source.single("!")) :: Nil
        case bm: BinaryMessage =>
          // ignore binary messages but drain content to avoid the stream being clogged
          bm.dataStream.runWith(Sink.ignore)
          Nil
      }

    val websocketRoute: Route =
      path("echo") {
        handleWebSocketMessages(echoFlow)
      }

    val serverBinding = Http().bindAndHandle(websocketRoute, address, port)
    serverBinding.onComplete {
      case Success(b) =>
        println("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        println(s"Server could not bind to $address:$port. Exception message: ${e.getMessage}")
        system.terminate()
    }
  }

  def browserClient() = {
    val os = System.getProperty("os.name").toLowerCase
    if (os == "mac os x") Process("open ./src/main/resources/WebsocketEcho.html").!
  }
}

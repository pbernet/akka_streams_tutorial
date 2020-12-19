package alpakka.env

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.WebSocketDirectives
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
  * Websocket echo server
  *
  */
object WebsocketServer extends App with WebSocketDirectives {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("WebsocketServer")
  implicit val executionContext = system.dispatcher

  val (address, port) = ("127.0.0.1", 6002)
  server(address, port)

  def server(address: String, port: Int) = {

    def echoFlow: Flow[Message, Message, Any] =
      Flow[Message].mapConcat {
        case tm: TextMessage =>
          logger.info(s"Server received: $tm")
          TextMessage(Source.single("Echo: ") ++ tm.textStream) :: Nil
        case bm: BinaryMessage =>
          // ignore binary messages but drain content to avoid the stream being clogged
          bm.dataStream.runWith(Sink.ignore)
          Nil
      }

    val websocketRoute: Route =
      path("echo") {
        handleWebSocketMessages(echoFlow)
      }

    val bindingFuture = Http().newServerAt(address, port).bindFlow(websocketRoute)
    bindingFuture.onComplete {
      case Success(b) =>
        logger.info("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        logger.info(s"Server could not bind to $address:$port. Exception message: ${e.getMessage}")
        system.terminate()
    }

    sys.addShutdownHook {
      logger.info("About to shutdown...")
      val fut = bindingFuture.map(serverBinding => serverBinding.terminate(hardDeadline = 3.seconds))
      logger.info("Waiting for connections to terminate...")
      val onceAllConnectionsTerminated = Await.result(fut, 10.seconds)
      logger.info("Connections terminated")
      onceAllConnectionsTerminated.flatMap { _ => system.terminate()
      }
    }
  }
}

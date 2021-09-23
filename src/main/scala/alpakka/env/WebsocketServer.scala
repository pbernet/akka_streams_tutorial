package alpakka.env

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.WebSocketDirectives
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
  * Websocket echo server
  *
  */
class WebsocketServer extends WebSocketDirectives {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val (address, port) = ("127.0.0.1", 6002)
  var serverBinding: Future[Http.ServerBinding] = _

  def run() = {
    server(address, port)
  }

  def stop() = {
    logger.info("About to shutdown...")
    val fut = serverBinding.map(serverBinding => serverBinding.terminate(hardDeadline = 3.seconds))
    logger.info("Waiting for connections to terminate...")
    val onceAllConnectionsTerminated = Await.result(fut, 10.seconds)
    logger.info("Connections terminated")
    onceAllConnectionsTerminated.flatMap { _ => system.terminate()
    }
  }

  private def server(address: String, port: Int) = {

    def echoFlow: Flow[Message, Message, Any] =
      Flow[Message].mapConcat {
        case tm: TextMessage =>
          logger.info(s"WebsocketServer received: $tm")
          TextMessage(Source.single("ACK: ") ++ tm.textStream) :: Nil
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
        logger.info("WebsocketServer started, listening on: " + b.localAddress)
        serverBinding = bindingFuture
      case Failure(e) =>
        logger.info(s"Server could not bind to $address:$port. Exception message: ${e.getMessage}")
        stop()
    }
  }

  sys.ShutdownHookThread {
    logger.info("Got control-c cmd from shell or SIGTERM, about to shutdown...")
    stop()
  }
}

object WebsocketServer extends App {
  val server = new WebsocketServer()
  server.run()

  def apply() = new WebsocketServer()

  def stop() = server.stop()
}
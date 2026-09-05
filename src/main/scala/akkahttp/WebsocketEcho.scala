package akkahttp

import org.apache.pekko.Done
import org.apache.pekko.actor.{ActorRef, ActorSystem}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.ws.*
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.server.directives.WebSocketDirectives
import org.apache.pekko.pattern.ask
import org.apache.pekko.stream.CompletionStrategy
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.pekko.util.Timeout
import org.slf4j.{Logger, LoggerFactory}
import sttp.client3.pekkohttp.PekkoHttpBackend
import sttp.client3.{UriContext, asWebSocket, basicRequest}
import sttp.ws.WebSocket

import java.time.LocalDateTime
import scala.collection.parallel.CollectionConverters.*
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future, Promise}
import scala.language.postfixOps
import scala.sys.process.{Process, stringSeqToProcess}
import scala.util.{Failure, Success}

trait ClientCommon {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit lazy val system: ActorSystem = ActorSystem()
  implicit lazy val executionContext: ExecutionContextExecutor = system.dispatcher

  val printSink: Sink[Message, Future[Done]] =
    Sink.foreach {
      //see https://github.com/akka/akka-http/issues/65
      case TextMessage.Strict(text) => logger.info(s"Client received TextMessage.Strict: $text")
      case TextMessage.Streamed(textStream) => textStream.runFold("")(_ + _).onComplete(value => logger.info(s"Client received TextMessage.Streamed: ${value.get}"))
      case BinaryMessage.Strict(_) => // binary, do nothing
      case BinaryMessage.Streamed(binaryStream) => binaryStream.runWith(Sink.ignore)
    }

  // see https://doc.akka.io/docs/akka-http/current/client-side/websocket-support.html?language=scala#half-closed-websockets
  def namedSource(clientname: String): Source[Message, Promise[Option[Message]]] = {
    Source
      .tick(1.second, 1.second, "tick")
      .zipWithIndex
      .map { case (_, i) => i }
      .map(i => TextMessage(s"$clientname-$i"))
      //.take(2)
      .concatMat(Source.maybe[Message])(Keep.right)
  }

  def browserClient(): AnyVal = {
    val os = System.getProperty("os.name").toLowerCase
    if (os == "mac os x") Process("open src/main/resources/WebsocketEcho.html").!
    else if (os.startsWith("windows")) Seq("cmd", "/c", "start src/main/resources/WebsocketEcho.html").!
  }
}

/**
  * Websocket echo example with different client types
  * Each client instance produces its own `echoFlow` on the server
  *
  * Clients do not close (implicitly) due to config:
  * `http.server.websocket.periodic-keep-alive-max-idle`
  * see file `application.conf` for details
  *
  * Like akka-http, pekko-http has no built-in API for websocket close
  * see: https://github.com/akka/akka-http/issues/2458
  *
  * Already implemented explicit client closing patterns:
  *  - [[akkahttp.WebsocketEcho.serverHeartbeatStreamClient]] shows an explicit client closing scenario (also from Browser)
  *    Inspired by: https://discuss.lightbend.com/t/websocket-connection-does-not-terminate-even-when-client-tries-to-close-it/8285
  *  - [[akkahttp.WebsocketEcho.singleWebSocketRequestBackpressureClient]]
  *  - [[akkahttp.WebsocketEcho.actorClient]]
  *
  * See "Windturbine Example" in pkg [[sample.stream_actor]] for more life cycle management and fault-tolerance behaviour
  */
object WebsocketEcho extends WebSocketDirectives with ClientCommon {
  val (address, port) = ("127.0.0.1", 6002)
  val maxClients = 2

  def main(args: Array[String]): Unit = {
    server(address, port)
    browserClient()

    // Comment out to see behavior of each client type
    (1 to maxClients).par.foreach(each => singleWebSocketRequestClient(each, address, port))
    (1 to maxClients).par.foreach(each => webSocketClientFlowClient(each, address, port))
    (1 to maxClients).par.foreach(each => singleWebSocketRequestBackpressureClient(each, address, port))
    (1 to maxClients).par.foreach(each => actorClient(each, address, port))
    (1 to maxClients).par.foreach(each => sttpClient(each, address, port))

    (1 to maxClients).par.foreach(each => serverHeartbeatStreamClient(each, address, port))
  }

  def server(address: String, port: Int) = {

    // This flow does not terminate when client terminates
    def echoFlow: Flow[Message, Message, Any] =
      Flow[Message].mapConcat {
        case tm: TextMessage =>
          logger.info(s"Server received: $tm")
          // This is important (regarding termination):
          // Stream back the TextMessage as the tail of the response
          // this means we might start sending the response even before the
          // end of the incoming message has been received
          TextMessage(Source.single("Hello ") ++ tm.textStream ++ Source.single("!")) :: Nil
        case bm: BinaryMessage =>
          // Ignore binary messages but drain content to avoid the stream being clogged
          bm.dataStream.runWith(Sink.ignore)
          Nil
      }
        .watchTermination((_, done) => done.onComplete {
          case Failure(err) => logger.info(s"Echo server flow failed: $err")
          case _ => logger.info(s"Echo server flow terminated")
        })

    def getEcho: Route = {
      path("echo") {
        extractRequest { request =>
          logger.info(s"Got echo request from client: ${request.getHeader("User-Agent")}")
          handleWebSocketMessages(echoFlow)
        }
      }
    }

    def getEchoHeartbeat: Route = {
      path("echo_heartbeat") {
        extractRequest { request =>
          logger.info(s"Got echo_heartbeat request from client: ${request.getHeader("User-Agent")}")

          // The inSink and the outSource are independent. By using fromSinkAndSourceCoupled
          // we kill the outSource once we get a terminate signal from the inSink
          // https://stackoverflow.com/questions/54097587/stop-akka-stream-source-when-web-socket-connection-is-closed-by-the-client

          val outSource =
            Source
              .repeat(s"Heartbeat response: ${LocalDateTime.now()}")
              .throttle(1, 1.seconds)
              .wireTap(msg => logger.info(s"Sending to client: $msg"))
              .map(TextMessage.Strict.apply)
              .watchTermination((_, done) => done.onComplete {
                case Failure(err) => logger.info(s"Heartbeat server flow failed: $err")
                case _ => logger.info(s"Heartbeat server flow terminated")
              })

          extractWebSocketUpgrade { upgrade =>
            val inSink = Sink.onComplete(_ => logger.info("Client signaled termination, shutdown corresponding echo_heartbeat server flow..."))
            complete(upgrade.handleMessages(Flow.fromSinkAndSourceCoupled(inSink, outSource), subprotocol = None))
          }
        }
      }
    }

    def routes: Route = {
      getEcho ~ getEchoHeartbeat
    }

    val bindingFuture = Http().newServerAt(address, port).bindFlow(routes)
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

  def singleWebSocketRequestClient(id: Int, address: String, port: Int): Unit = {

    val webSocketNonReusableFlow: Flow[Message, Message, Promise[Option[Message]]] =
      Flow.fromSinkAndSourceMat(
        printSink,
        namedSource(id.toString))(Keep.right)

    val (upgradeResponse, completionPromise: Promise[Option[Message]]) =
      Http().singleWebSocketRequest(WebSocketRequest(s"ws://$address:$port/echo"), webSocketNonReusableFlow)

    val connected = handleUpgrade(upgradeResponse)

    connected.onComplete(done => logger.info(s"Client: $id singleWebSocketRequestClient connected: $done"))
    completionPromise.future.onComplete(closed => logger.info(s"Client: $id singleWebSocketRequestClient closed: $closed"))
  }

  def webSocketClientFlowClient(id: Int, address: String, port: Int): Unit = {

    val webSocketNonReusableFlow: Flow[Message, Message, Future[WebSocketUpgradeResponse]] = Http().webSocketClientFlow(WebSocketRequest(s"ws://$address:$port/echo"))

    val (upgradeResponse, closed) =
      namedSource(id.toString)
        .viaMat(webSocketNonReusableFlow)(Keep.right) // keep the materialized Future[WebSocketUpgradeResponse]
        .toMat(printSink)(Keep.both) // also keep the Future[Done]
        .run()

    val connected = handleUpgrade(upgradeResponse)

    connected.onComplete(done => logger.info(s"Client: $id webSocketClientFlowClient connected: $done"))
    closed.onComplete(closed => logger.info(s"Client: $id webSocketClientFlowClient closed: $closed"))
  }

  def singleWebSocketRequestBackpressureClient(id: Int, address: String, port: Int): Unit = {
    val source = Source.actorRefWithBackpressure[Message](
        ackMessage = "ack",
        completionMatcher = {
          case Done => CompletionStrategy.immediately
        },
        failureMatcher = PartialFunction.empty)
      .watchTermination(Keep.both)

    val webSocketNonReusableFlow = Flow.fromSinkAndSourceMat(printSink, source)(Keep.right)

    val (upgradeResponse, (sendToSocketRef, streamCompletion)) =
      Http().singleWebSocketRequest(WebSocketRequest(s"ws://$address:$port/echo"), webSocketNonReusableFlow)

    val connected = handleUpgrade(upgradeResponse)

    connected.onComplete(done => logger.info(s"Client: $id singleWebSocketRequestBackpressureClient connected: $done"))
    streamCompletion.onComplete(closed => logger.info(s"Client: $id singleWebSocketRequestBackpressureClient closed: $closed"))

    def send(messageText: String) = {
      val message = TextMessage.Strict(messageText)
      implicit val timeout: Timeout = Timeout(30.seconds)
      sendToSocketRef.ask(message).map(_ => logger.info(s"sent $message"))
    }

    send(s"$id-1 SourceQueueClient")
      .flatMap(_ => send(s"$id-2 SourceQueueClient"))
      .onComplete { _ =>
        logger.info(s"About to explicitly close client: $id...")
        sendToSocketRef ! Done
      }
  }

  def actorClient(id: Int, address: String, port: Int): Unit = {

    val sourceBackpressure = Source.actorRefWithBackpressure[TextMessage](
      ackMessage = "ack",
      completionMatcher = {
        case Done =>
          logger.info("ActorClient: close connection")
          CompletionStrategy.immediately
      },
      failureMatcher = PartialFunction.empty)

    val webSocketNonReusableFlow = Flow.fromSinkAndSourceMat(printSink, sourceBackpressure)(Keep.right)

    val (upgradeResponse, _) =
      Http().singleWebSocketRequest(WebSocketRequest(s"ws://$address:$port/echo"), webSocketNonReusableFlow)

    val connected = handleUpgrade(upgradeResponse)

    connected.onComplete(done => logger.info(s"ActorClient: $id connected: $done"))

    val (sendToSocketRef: ActorRef, _) =
      sourceBackpressure
        .viaMat(webSocketNonReusableFlow)(Keep.both)
        .toMat(printSink)(Keep.left)
        .run()

    implicit val askTimeout: Timeout = Timeout(30.seconds)
    sendToSocketRef.ask(TextMessage(s"$id-1 ActorClient"))
    sendToSocketRef.ask(TextMessage(s"$id-2 ActorClient"))
    sendToSocketRef ! Done
  }

  // The STTP client wins the "conciseness award"
  // https://github.com/softwaremill/sttp
  def sttpClient(id: Int, address: String, port: Int) = {

    def useWebSocket(ws: WebSocket[Future]): Future[Unit] = {
      def send(payload: String) = ws.sendText(payload)
      def receive() = ws.receiveText().map(t => logger.info(s"sttpClient $id received: $t"))

      val messages = (1 to 5).map(i => s"$id-$i sttpClient")

      messages.foldLeft(Future.successful(())) { (prev, msg) =>
        prev.flatMap { _ =>
          for {
            _ <- send(msg)
            _ <- receive()
          } yield ()
        }
      }
    }

    val backend = PekkoHttpBackend()

    basicRequest
      .response(asWebSocket(useWebSocket))
      .get(uri"ws://$address:$port/echo")
      .send(backend)
      //.onComplete(_ => backend.close())
  }

  def serverHeartbeatStreamClient(id: Int, address: String, port: Int) = {
    val webSocketNonReusableFlow: Flow[Message, Message, Promise[Option[Message]]] = {
      Flow.fromSinkAndSourceMat(
        printSink,
        Source.maybe[Message])(Keep.right)
    }

    val (upgradeResponse, completionPromise: Promise[Option[Message]]) =
      Http().singleWebSocketRequest(WebSocketRequest(s"ws://$address:$port/echo_heartbeat"), webSocketNonReusableFlow)

    val connected = handleUpgrade(upgradeResponse)

    connected.onComplete(done => logger.info(s"Client: $id serverHeartbeatStreamClient connected: $done"))
    completionPromise.future.onComplete(closed => logger.info(s"Client: $id serverHeartbeatStreamClient closed: $closed"))

    Thread.sleep(10000)
    logger.info(s"About to explicitly close client: $id...")
    completionPromise.success(None)
  }

  private def handleUpgrade(upgradeResponse: Future[WebSocketUpgradeResponse]) = {
    upgradeResponse.map { upgrade =>
      // Status code 101 (= Switching Protocols) indicates that server support WebSockets
      if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
        Done
      } else {
        throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
      }
    }
  }
}

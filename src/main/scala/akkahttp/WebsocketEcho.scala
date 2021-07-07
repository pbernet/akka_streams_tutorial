package akkahttp

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.ws.{TextMessage, _}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.WebSocketDirectives
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueue}
import akka.stream.{KillSwitches, OverflowStrategy, QueueOfferResult}

import java.time.LocalDateTime
import scala.collection.parallel.CollectionConverters._
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}
import scala.language.postfixOps
import scala.sys.process.Process
import scala.util.{Failure, Success}

trait ClientCommon {
  implicit val system = ActorSystem("Websocket")
  implicit val executionContext = system.dispatcher

  val printSink: Sink[Message, Future[Done]] =
    Sink.foreach {
      //see https://github.com/akka/akka-http/issues/65
      case TextMessage.Strict(text) => println(s"Client received TextMessage.Strict: $text")
      case TextMessage.Streamed(textStream) => textStream.runFold("")(_ + _).onComplete(value => println(s"Client received TextMessage.Streamed: ${value.get}"))
      case BinaryMessage.Strict(binary) => //do nothing
      case BinaryMessage.Streamed(binaryStream) => binaryStream.runWith(Sink.ignore)
    }

  //see https://doc.akka.io/docs/akka-http/current/client-side/websocket-support.html?language=scala#half-closed-websockets
  def namedSource(clientname: String) = {
    Source
      .tick(1.second, 1.second, "tick")
      .zipWithIndex
      .map { case (_, i) => i }
      .map(i => TextMessage(s"$clientname-$i"))
      //.take(2)
      .concatMat(Source.maybe[Message])(Keep.right)
  }

  def browserClient() = {
    val os = System.getProperty("os.name").toLowerCase
    if (os == "mac os x") Process("open src/main/resources/WebsocketEcho.html").!
  }

}

/**
  * Websocket echo example with different client types
  * Each client instance produces it's own echoFlow on the server
  *
  * Clients do not close (implicitly) due to config:
  * `http.server.websocket.periodic-keep-alive-max-idle`
  * see file `application.conf` for details
  *
  * However, [[akkahttp.WebsocketEcho.serverHeartbeatStreamClient]] shows explicit closing
  * see: https://discuss.lightbend.com/t/websocket-connection-does-not-terminate-even-when-client-tries-to-close-it/8285
  *
  * See "Windturbine Example" in pkg [[sample.stream_actor]] for more life cycle management and fault-tolerance behaviour
  */
object WebsocketEcho extends App with WebSocketDirectives with ClientCommon {

  val (address, port) = ("127.0.0.1", 6002)
  server(address, port)
  browserClient()

  // Comment out to see behaviour of each client type
  val maxClients = 2
  (1 to maxClients).par.foreach(each => singleWebSocketRequestClient(each, address, port))
  (1 to maxClients).par.foreach(each => webSocketClientFlowClient(each, address, port))
  (1 to maxClients).par.foreach(each => singleWebSocketRequestSourceQueueClient(each, address, port))
  (1 to maxClients).par.foreach(each => serverHeartbeatStreamClient(each, address, port))

  def server(address: String, port: Int) = {

    def echoFlow: Flow[Message, Message, Any] =
      Flow[Message].mapConcat {
        case tm: TextMessage =>
          println(s"Server received: $tm")
          TextMessage(Source.single("Hello ") ++ tm.textStream ++ Source.single("!")) :: Nil
        case bm: BinaryMessage =>
          // Ignore binary messages but drain content to avoid the stream being clogged
          bm.dataStream.runWith(Sink.ignore)
          Nil
      }
        .watchTermination()((_, done) => done.onComplete {
          case Failure(err) => println(s"Echo server flow failed: $err")
          case _ => println(s"Echo server flow terminated")
        })

    def getEcho: Route = {
      path("echo") {
        extractRequest { request =>
          println(s"Got echo request from client: ${request.getHeader("User-Agent")}")
          handleWebSocketMessages(echoFlow)
        }
      }
    }

    def getEchoHeartbeat: Route = {
      path("echo_heartbeat") {
        extractRequest { request =>
          println(s"Got echo_heartbeat request from client: ${request.getHeader("User-Agent")}")

          // Because the inSink and the outSource are independent, we need a kill-switch
          // to kill the outSource once we get a terminate signal from the inSink
          // https://stackoverflow.com/questions/54097587/stop-akka-stream-source-when-web-socket-connection-is-closed-by-the-client

          val sharedKillSwitch = KillSwitches.shared("kill-switch")
          val outSource =
            Source
              .repeat(s"Heartbeat response: ${LocalDateTime.now()}")
              .throttle(1, 1.seconds)
              .wireTap(msg => println(s"Sending to client: $msg"))
              .map(TextMessage.Strict)
              .via(sharedKillSwitch.flow)
              .watchTermination()((_, done) => done.onComplete {
                case Failure(err) => println(s"Heartbeat server flow failed: $err")
                case _ => println(s"Heartbeat server flow terminated")
              })

          extractWebSocketUpgrade { upgrade =>
            val inSink = Sink.onComplete(_ => {
              println("Client signaled termination, shutdown server flow...")
              sharedKillSwitch.shutdown()
            })
            complete(upgrade.handleMessagesWithSinkSource(inSink, outSource))
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
        println("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        println(s"Server could not bind to $address:$port. Exception message: ${e.getMessage}")
        system.terminate()
    }

    sys.addShutdownHook {
      println("About to shutdown...")
      val fut = bindingFuture.map(serverBinding => serverBinding.terminate(hardDeadline = 3.seconds))
      println("Waiting for connections to terminate...")
      val onceAllConnectionsTerminated = Await.result(fut, 10.seconds)
      println("Connections terminated")
      onceAllConnectionsTerminated.flatMap { _ => system.terminate()
      }
    }
  }

  def singleWebSocketRequestClient(id: Int, address: String, port: Int) = {

    val webSocketNonReusableFlow: Flow[Message, Message, Promise[Option[Message]]] =
      Flow.fromSinkAndSourceMat(
        printSink,
        namedSource(id.toString))(Keep.right)

    val (upgradeResponse, sourceClosed) =
      Http().singleWebSocketRequest(WebSocketRequest(s"ws://$address:$port/echo"), webSocketNonReusableFlow)

    val connected = handleUpgrade(upgradeResponse)

    connected.onComplete(done => println(s"Client: $id singleWebSocketRequestClient connected: $done"))
    sourceClosed.future.onComplete(closed => println(s"Client: $id singleWebSocketRequestClient closed: $closed"))
  }


  def singleWebSocketRequestSourceQueueClient(id: Int, address: String, port: Int) = {

    val (source, sourceQueue) = {
      val p = Promise[SourceQueue[Message]]()
      val s = Source.queue[Message](100, OverflowStrategy.backpressure, 100).mapMaterializedValue(m => {
        p.trySuccess(m)
        m
      })
      (s, p.future)
    }

    val webSocketNonReusableFlow = Flow.fromSinkAndSourceMat(printSink, source)(Keep.right)

    val (upgradeResponse, sourceQueueWithComplete) =
      Http().singleWebSocketRequest(WebSocketRequest(s"ws://$address:$port/echo"), webSocketNonReusableFlow)

    val connected = handleUpgrade(upgradeResponse)

    connected.onComplete(done => println(s"Client: $id singleWebSocketRequestSourceQueueClient connected: $done"))
    sourceQueueWithComplete.watchCompletion().onComplete(closed => println(s"Client: $id singleWebSocketRequestSourceQueueClient closed: $closed"))

    def send(messageText: String) = {
      val message = TextMessage.Strict(messageText)
      sourceQueue.flatMap { queue =>
        queue.offer(message: Message).map {
          case QueueOfferResult.Enqueued => println(s"enqueued $message")
          case QueueOfferResult.Dropped => println(s"dropped $message")
          case QueueOfferResult.Failure(ex) => println(s"Offer failed: $ex")
          case QueueOfferResult.QueueClosed => println("Source Queue closed")
        }
      }
    }

    send(s"$id-1 SourceQueue")
    send(s"$id-2 SourceQueue")
  }

  def webSocketClientFlowClient(id: Int, address: String, port: Int) = {

    val webSocketNonReusableFlow: Flow[Message, Message, Future[WebSocketUpgradeResponse]] = Http().webSocketClientFlow(WebSocketRequest(s"ws://$address:$port/echo"))

    val (upgradeResponse, closed) =
      namedSource(id.toString)
        .viaMat(webSocketNonReusableFlow)(Keep.right) // keep the materialized Future[WebSocketUpgradeResponse]
        .toMat(printSink)(Keep.both) // also keep the Future[Done]
        .run()

    val connected = handleUpgrade(upgradeResponse)

    connected.onComplete(done => println(s"Client: $id webSocketClientFlowClient connected: $done"))
    closed.onComplete(closed => println(s"Client: $id webSocketClientFlowClient closed: $closed"))
  }

  def serverHeartbeatStreamClient(id: Int, address: String, port: Int) = {
    val sourceKickOff = Source
      .single(TextMessage("kick off msg"))
      .concatMat(Source.maybe[Message])(Keep.right)

    val webSocketNonReusableFlow: Flow[Message, Message, Promise[Option[Message]]] = {
      Flow.fromSinkAndSourceMat(
        printSink,
        sourceKickOff)(Keep.right)
    }

    val (upgradeResponse, sourceClosed: Promise[Option[Message]]) =
      Http().singleWebSocketRequest(WebSocketRequest(s"ws://$address:$port/echo_heartbeat"), webSocketNonReusableFlow)

    val connected = handleUpgrade(upgradeResponse)

    connected.onComplete(done => println(s"Client: $id serverHeartbeatStreamClient connected: $done"))
    sourceClosed.future.onComplete(closed => println(s"Client: $id serverHeartbeatStreamClient closed: $closed"))

    Thread.sleep(10000)
    println(s"About to close client: $id...")
    sourceClosed.success(None)
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

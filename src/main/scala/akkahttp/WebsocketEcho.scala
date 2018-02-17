package akkahttp

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.WebSocketDirectives
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.github.andyglow.websocket._

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}
import scala.language.postfixOps
import scala.util.{Failure, Success}


trait ClientCommon {
  implicit val system = ActorSystem("WebsocketEcho")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  val printSink: Sink[Message, Future[Done]] =
    Sink.foreach {
      //see https://github.com/akka/akka-http/issues/65
      case TextMessage.Strict(text) => println(s"Client received TextMessage.Strict: $text")
      case TextMessage.Streamed(textStream) => textStream.runFold("")(_ + _).onComplete(value => println(s"Client received TextMessage.Streamed: ${value.get}"))
      case BinaryMessage.Strict(binary) => //do nothing
      case BinaryMessage.Streamed(binaryStream) => binaryStream.runWith(Sink.ignore)
    }

  //see http://doc.akka.io/docs/akka-http/10.0.10/scala/http/client-side/websocket-support.html#half-closed-client-websockets
  val helloSource = Source(List(TextMessage("world one"), TextMessage("world two")))
    .concatMat(Source.maybe[Message])(Keep.right)
}

/**
  * Websocket echo example with different client types
  * Each client instance produces it's own echoFlow on the server
  *
  */
object WebsocketEcho extends App with WebSocketDirectives with ClientCommon {

  val (address, port) = ("127.0.0.1", 6000)
  server(address, port)
  (1 to 2).par.foreach(each => clientNettyBased(each, address, port))
  (1 to 2).par.foreach(each => clientSingleWebSocketRequest(each, address, port))
  (1 to 2).par.foreach(each => clientWebSocketClientFlow(each, address, port))

  private def server(address: String, port: Int) = {

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

    sys.addShutdownHook {
      serverBinding.map(b => b.unbind().onComplete(_ => println("Unbound server, about to terminate...")))
      system.terminate()
      Await.result(system.whenTerminated, 30.seconds)
      println("Terminated... Bye")
    }
  }

  private def clientNettyBased(id: Int, address: String, port: Int) = {

    // see https://github.com/andyglow/websocket-scala-client
    val cli = WebsocketClient[String](s"ws://$address:$port/echo") {
      case str => println(s"Client: $id NettyBased received String: $str")
    }
    val ws = cli.open()
    ws ! "world one"
    ws ! "world two"
    Thread.sleep(5000)
    val done = cli.shutdownAsync
    done.onComplete(closed => println(s"Client: $id NettyBased closed: $closed"))
  }

  private def clientSingleWebSocketRequest(id: Int, address: String, port: Int) = {

    // flow to use (note: not re-usable!)
    val webSocketFlow: Flow[Message, Message, Promise[Option[Message]]] =
      Flow.fromSinkAndSourceMat(
        printSink,
        helloSource)(Keep.right)

    val (upgradeResponse: Future[WebSocketUpgradeResponse], closed: Promise[Option[Message]]) =
      Http().singleWebSocketRequest(WebSocketRequest(s"ws://$address:$port/echo"), webSocketFlow)

    val connected = handleUpgrade(upgradeResponse)

    connected.onComplete(done => println(s"Client: $id SingleWebSocketRequest connected: $done"))
    //Closes on TCP idle-timeout (default is 1 minute)
    closed.future.onComplete(closed => println(s"Client: $id SingleWebSocketRequest closed: $closed"))
  }

  private def clientWebSocketClientFlow(id: Int, address: String, port: Int) = {

    val webSocketNonReusableFlow = Http().webSocketClientFlow(WebSocketRequest(s"ws://$address:$port/echo"))

    val (upgradeResponse: Future[WebSocketUpgradeResponse], closed: Future[Done]) =
      helloSource
        .viaMat(webSocketNonReusableFlow)(Keep.right) // keep the materialized Future[WebSocketUpgradeResponse]
        .toMat(printSink)(Keep.both) // also keep the Future[Done]
        .run()

    val connected = handleUpgrade(upgradeResponse)

    connected.onComplete(done => println(s"clientWebSocketClientFlow connected: $done"))
    closed.onComplete(closed => println(s"Client: $id SingleWebSocketRequest closed: $closed"))
  }

  private def handleUpgrade(upgradeResponse: Future[WebSocketUpgradeResponse]) = {
    upgradeResponse.map { upgrade =>
      // status code 101 (Switching Protocols) indicates that server support WebSockets
      if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
        Done
      } else {
        throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
      }
    }
  }
}

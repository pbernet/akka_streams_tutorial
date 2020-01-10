package akkahttp

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.WebSocketDirectives
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}

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

  //see https://doc.akka.io/docs/akka-http/10.1.8/client-side/websocket-support.html?language=scala#half-closed-websockets
  def namedSource(clientname: String) = {
    Source
      .tick(1.second, 1.second, "tick")
      .zipWithIndex
      .map { case (_, i) => i }
      .map(i => TextMessage(s"$clientname-$i"))
      //.take(2)
      .concatMat(Source.maybe[Message])(Keep.right)
  }

}

/**
  * Websocket echo example with different client types
  * Each client instance produces it's own echoFlow on the server
  *
  * Please note that this basic example has no life cycle management nor fault-tolerance
  * The "Windturbine Example" does show this
  *
  */
object WebsocketEcho extends App with WebSocketDirectives with ClientCommon {

  val (address, port) = ("127.0.0.1", 6002)
  server(address, port)
  browserClient()
  val maxClients = 2
  (1 to maxClients).par.foreach(each => singleWebSocketRequestClient(each, address, port))
  (1 to maxClients).par.foreach(each => webSocketClientFlowClient(each, address, port))

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

    sys.addShutdownHook {
      serverBinding.map(b => b.unbind().onComplete(_ => println("Unbound server, about to terminate...")))
      system.terminate()
      Await.result(system.whenTerminated, 30.seconds)
      println("Terminated... Bye")
    }
  }

  private def browserClient() = {
    val os = System.getProperty("os.name").toLowerCase
    if (os == "mac os x") Process("open ./src/main/resources/WebsocketEcho.html").!
  }


  def singleWebSocketRequestClient(id: Int, address: String, port: Int) = {

    val webSocketNonReusableFlow: Flow[Message, Message, Promise[Option[Message]]] =
      Flow.fromSinkAndSourceMat(
        printSink,
        namedSource(id.toString))(Keep.right)

    val (upgradeResponse: Future[WebSocketUpgradeResponse], closed: Promise[Option[Message]]) =
      Http().singleWebSocketRequest(WebSocketRequest(s"ws://$address:$port/echo"), webSocketNonReusableFlow)

    val connected = handleUpgrade(upgradeResponse)

    connected.onComplete(done => println(s"Client: $id singleWebSocketRequestClient connected: $done"))
    //Does not close anymore due to new configurable "automatic keep-alive Ping support" - see application.conf
    closed.future.onComplete(closed => println(s"Client: $id singleWebSocketRequestClient closed: $closed"))
  }

  def webSocketClientFlowClient(id: Int, address: String, port: Int) = {

    val webSocketNonReusableFlow: Flow[Message, Message, Future[WebSocketUpgradeResponse]] = Http().webSocketClientFlow(WebSocketRequest(s"ws://$address:$port/echo"))

    val (upgradeResponse: Future[WebSocketUpgradeResponse], closed: Future[Done]) =
      namedSource(id.toString)
        .viaMat(webSocketNonReusableFlow)(Keep.right) // keep the materialized Future[WebSocketUpgradeResponse]
        .toMat(printSink)(Keep.both) // also keep the Future[Done]
        .run()

    val connected = handleUpgrade(upgradeResponse)

    connected.onComplete(done => println(s"Client: $id webSocketClientFlowClient connected: $done"))
    //Does not close anymore due to new configurable "automatic keep-alive Ping support" - see application.conf
    closed.onComplete(closed => println(s"Client: $id webSocketClientFlowClient closed: $closed"))
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

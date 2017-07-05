package akkahttp

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{BroadcastHub, Flow, Keep, MergeHub, Sink, Source}
import akka.{Done, NotUsed}
import akkahttp.WebsocketEcho.{helloSource, printSink}

import scala.concurrent.Future

/**
  * A simple WebSocket chat system using only Akka Streams with the help of MergeHubSource and BroadcastHub Sink
  * Shamelessly copied from:
  * https://github.com/calvinlfer/akka-http-streaming-response-examples/blob/master/src/main/scala/com/experiments/calvin/WebsocketStreamsMain.scala
  * Doc: http://doc.akka.io/docs/akka/current/scala/stream/stream-dynamic.html#dynamic-fan-in-and-fan-out-with-mergehub-and-broadcasthub
  */
object WebsocketChatEcho {
  implicit val actorSystem = ActorSystem(name = "example-actor-system")
  implicit val streamMaterializer = ActorMaterializer()
  implicit val executionContext = actorSystem.dispatcher
  val log: LoggingAdapter = actorSystem.log


  def main(args: Array[String]) {
    val (address, port) = ("127.0.0.1", 6000)
    val maxClients = 10
    server(address, port)
    for ( a <- 1 to maxClients) clientWebSocketClientFlow(address, port)  //Resulting messages in stdout: 2 * maxClients^2
  }

  private def server(address: String, port: Int) = {

    /*
  many clients -> Merge Hub -> Broadcast Hub -> many clients
  Visually
                                                                                                         Akka Streams Flow
                  ________________________________________________________________________________________________________________________________________________________________________________________
  c1 -------->\  |                                                                                                                                                                                        |  /->----------- c1
               \ |                                                                                                                                                                                        | /
  c2 ----------->| Sink ========================(feeds data to)===========> MergeHub Source ->-->-->--> BroadcastHub Sink ======(feeds data to)===========> Source                                        |->->------------ c2
                /| that comes from materializing the                                        connected to                                                    that comes from materializing the             | \
               / | MergeHub Source                                                                                                                          BroadcastHub Sink                             |  \
  c3 -------->/  |________________________________________________________________________________________________________________________________________________________________________________________|   \->---------- c3


  Runnable Flow (MergeHubSource -> BroadcastHubSink)

  Materializing a MergeHub Source yields a Sink that collects all the emitted elements and emits them in the MergeHub Source (the emitted elements that are collected in the Sink are coming from all WebSocket clients)
  Materializing a BroadcastHub Sink yields a Source that broadcasts all elements being collected by the MergeHub Sink (the elements that are emitted/broadcasted in the Source are going to all WebSocket clients)
   */
    val (chatSink: Sink[String, NotUsed], chatSource: Source[String, NotUsed]) =
      MergeHub.source[String].toMat(BroadcastHub.sink[String])(Keep.both).run()

    val echoFlow: Flow[Message, Message, NotUsed] =
    Flow[Message].mapAsync(1) {
      case TextMessage.Strict(text) =>
        println(s"Server recieved: $text")
        Future.successful(text)
      case streamed: TextMessage.Streamed => streamed.textStream.runFold("") {
        (acc, next) => acc ++ next
      }
    }
      .via(Flow.fromSinkAndSource(chatSink, chatSource))
      .map[Message](string => TextMessage.Strict("Hello " + string + "!"))

    def wsChatStreamsOnlyRoute =
      path("echochat") {
        handleWebSocketMessages(echoFlow)
      }

    val bindingFuture = Http().bindAndHandle(wsChatStreamsOnlyRoute, address, port)
    bindingFuture
      .map(_.localAddress)
      .map(addr => s"Bound to $addr")
      .foreach(log.info)
  }

  private def clientWebSocketClientFlow(address: String, port: Int) = {

    // flow to use (note: not re-usable!)
    val webSocketFlow: Flow[Message, Message, Future[WebSocketUpgradeResponse]] = Http().webSocketClientFlow(WebSocketRequest(s"ws://$address:$port/echochat"))

    val (upgradeResponse, closed) =
      helloSource
        .viaMat(webSocketFlow)(Keep.right) // keep the materialized Future[WebSocketUpgradeResponse]
        .toMat(printSink)(Keep.both) // also keep the Future[Done]
        .run()


    val connected = upgradeResponse.flatMap { upgrade =>
      // status code 101 (Switching Protocols) indicates that server support WebSockets
      if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
        Future.successful(Done)
      } else {
        throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
      }
    }

    connected.onComplete(println)
    closed.foreach(_ => println("closed"))
  }


}

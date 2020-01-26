package akkahttp

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.{BroadcastHub, Flow, Keep, MergeHub, Sink, Source}
import akka.{Done, NotUsed}
import akkahttp.WebsocketEcho.handleWebSocketMessages

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * A simple WebSocket chat system using only akka streams with the help of MergeHub Source and BroadcastHub Sink
  *
  * Shamelessly copied from:
  * https://github.com/calvinlfer/akka-http-streaming-response-examples/blob/master/src/main/scala/com/experiments/calvin/WebsocketStreamsMain.scala
  * Doc:
  * http://doc.akka.io/docs/akka/current/scala/stream/stream-dynamic.html#dynamic-fan-in-and-fan-out-with-mergehub-and-broadcasthub
  */
object WebsocketChatEcho extends App with ClientCommon {

    val (address, port) = ("127.0.0.1", 6002)
    server(address, port)
    browserClient()
    val clients = List("Bob", "Alice")
    clients.par.foreach(clientname => clientWebSocketClientFlow(clientname, address, port))

  private def server(address: String, port: Int) = {

    /*
  clients -> Merge Hub -> Broadcast Hub -> clients
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

    //Optional sample processing flow, to demonstrate the nature of the composition
    val sampleProcessing  = Flow[String].map(i => i.toUpperCase)

    val (chatSink: Sink[String, NotUsed], chatSource: Source[String, NotUsed]) =
      MergeHub.source[String]
        //.wireTap(elem => println(s"Server received after MergeHub: $elem"))
        .via(sampleProcessing)
        .toMat(BroadcastHub.sink[String])(Keep.both).run()

    val echoFlow: Flow[Message, Message, NotUsed] =
    Flow[Message].mapAsync(1) {
      case TextMessage.Strict(text) =>
        println(s"Server received: $text")
        Future.successful(text)
      case streamed: TextMessage.Streamed => streamed.textStream.runFold("") {
        (acc, next) => acc ++ next
      }
    }
      .via(Flow.fromSinkAndSource(chatSink, chatSource))
      //Add compression, without compression messages in stdout: numberOfMsg * maxClients^2
      .groupedWithin(10, 2.second)
      .map { eachSeq =>
        println(s"Compressed ${eachSeq.size} messages within 2 seconds")
        eachSeq.mkString("; ")
      }
      .map[Message](string => TextMessage.Strict("Hello " + string + "!"))

    def wsClientRoute: Route =
      path("echochat") {
        handleWebSocketMessages(echoFlow)
      }

    //The browser client has a different route but hooks into the same flow
    def wsBrowserClientRoute: Route =
      path("echo") {
        handleWebSocketMessages(echoFlow)
      }

    def routes: Route = {
      wsClientRoute ~ wsBrowserClientRoute
    }

    val bindingFuture = Http().bindAndHandle(routes, address, port)
    bindingFuture
      .map(_.localAddress)
      .map(addr => println(s"Server bound to: $addr"))
  }

  private def clientWebSocketClientFlow(clientname: String, address: String, port: Int) = {

    // flow to use (note: not re-usable!)
    val webSocketFlow: Flow[Message, Message, Future[WebSocketUpgradeResponse]] = Http().webSocketClientFlow(WebSocketRequest(s"ws://$address:$port/echochat"))

    val (upgradeResponse, closed) =
      namedSource(clientname)
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

    connected.onComplete(_ => println("client connected"))
    closed.foreach(_ => println("client closed"))
  }
}

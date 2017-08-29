package sample.stream

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Framing, Keep, Sink, Source, Tcp}
import akka.util.ByteString

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object TcpEcho {

  /**
   * Inspired by:
   * http://doc.akka.io/docs/akka/2.5.4/scala/stream/stream-io.html
   *
   * Use without parameters to start both server and 10 clients.
   *
   * Use parameters `server 0.0.0.0 6001` to start server listening on port 6001.
   *
   * Use parameters `client 127.0.0.1 6001` to start client connecting to
   * server on 127.0.0.1:6001.
   *
   */
  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      val system = ActorSystem("ClientAndServer")
      val (address, port) = ("127.0.0.1", 6000)
      server(system, address, port)
      (1 to 10).par.foreach(each => client(system, address, port))
    } else {
      val (address, port) =
        if (args.length == 3) (args(1), args(2).toInt)
        else ("127.0.0.1", 6000)
      if (args(0) == "server") {
        val system = ActorSystem("Server")
        server(system, address, port)
      } else if (args(0) == "client") {
        val system = ActorSystem("Client")
        client(system, address, port)
      }
    }
  }

  def server(system: ActorSystem, address: String, port: Int): Unit = {
    implicit val sys = system
    implicit val ec = system.dispatcher
    implicit val materializer = ActorMaterializer()

    val handler = Sink.foreach[Tcp.IncomingConnection] { connection =>

      // parse incoming commands and add ! at the end to show the client
      val commandParser = Flow[String].takeWhile(_ != "BYE").map(_ + "!")

      val welcomeMsg = s"Welcome to: ${connection.localAddress}, you are: ${connection.remoteAddress}!"
      val welcome = Source.single(welcomeMsg)

      val serverFlow = Flow[ByteString]
        .via(Framing.delimiter(  //chunk the inputs up into actual lines of text
          ByteString("\n"),
          maximumFrameLength = 256,
          allowTruncation = true))
        .map(_.utf8String)
        .via(commandParser)
        .merge(welcome) // merge in the initial banner after parser
        .map(_ + "\n")
        .map(ByteString(_))

      connection.handleWith(serverFlow)
    }

    //The idleTimeout setting results in this message on the client: "Connection reset by peer"
    val connections = Tcp().bind(interface = address, port = port, idleTimeout = 10.seconds)
    val (binding , done) = connections.watchTermination()(Keep.both).to(handler).run()

    binding.onComplete {
      case Success(b) =>
        println("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        println(s"Server could not bind to $address:$port: ${e.getMessage}")
        system.terminate()
    }

    //TODO This does not work. How would I shutdown the server on timeout, that is all clients are shutdown?
    //The doc says: It is also possible to shut down the server’s socket by cancelling the IncomingConnection source connections
    done.onComplete(_ => system.terminate())
  }

  def client(system: ActorSystem, address: String, port: Int): Unit = {
    implicit val sys = system
    implicit val ec = system.dispatcher
    implicit val materializer = ActorMaterializer()

    val connection: Flow[ByteString, ByteString, Future[Tcp.OutgoingConnection]] = Tcp().outgoingConnection(address, port)

    val testInput = ('a' to 'z').map(ByteString(_)) ++ Seq(ByteString("BYE"))

    val (conn: Future[Tcp.OutgoingConnection], closed: Future[Done]) =
      Source(testInput)
        .viaMat(connection)(Keep.right)
        .toMat(Sink.foreach(each => println(each.utf8String)))(Keep.both)
        .run()

    closed.onComplete(each => println(s"client closed: $each"))
  }
}

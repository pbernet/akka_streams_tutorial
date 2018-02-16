package sample.stream

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Framing, Keep, Sink, Source, Tcp}
import akka.util.ByteString

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

/**
  * Inspired by:
  * https://doc.akka.io/docs/akka/2.5.9/scala/stream/stream-io.html
  *
  * Use without parameters to start server and 10 parallel clients.
  *
  * Use parameters `server 0.0.0.0 6001` to start server listening on port 6001.
  *
  * Use parameters `client 127.0.0.1 6001` to start client connecting to
  * server on 127.0.0.1:6001.
  *
  */
object TcpEcho extends App {
  val system = ActorSystem("TcpEcho")
  var serverBinding: Future[Tcp.ServerBinding] = _

    if (args.isEmpty) {
      val (address, port) = ("127.0.0.1", 6000)
      serverBinding = server(system, address, port)
      (1 to 10).par.foreach(each => client(each, system, address, port))
    } else {
      val (address, port) =
        if (args.length == 3) (args(1), args(2).toInt)
        else ("127.0.0.1", 6000)
      if (args(0) == "server") {
        val system = ActorSystem("Server")
        serverBinding = server(system, address, port)
      } else if (args(0) == "client") {
        val system = ActorSystem("Client")
        client(1, system, address, port)
      }
    }

  sys.addShutdownHook{
    import scala.concurrent.ExecutionContext.Implicits.global
    serverBinding.map(b => b.unbind().onComplete(_ => println("Unbound server, about to terminate...")))
    system.terminate()
    Await.result(system.whenTerminated, 30.seconds)
    println("Terminated... Bye")
  }

  def server(system: ActorSystem, address: String, port: Int): Future[Tcp.ServerBinding] = {
    implicit val sys = system
    implicit val ec = system.dispatcher
    implicit val materializer = ActorMaterializer()

    val handler = Sink.foreach[Tcp.IncomingConnection] { connection =>

      // parse incoming commands and append !
      val commandParser = Flow[String].takeWhile(_ != "BYE").map(_ + "!")

      val welcomeMsg = s"Welcome to: ${connection.localAddress}, you are: ${connection.remoteAddress}!"
      val welcomeSource = Source.single(welcomeMsg)

      val serverEchoFlow = Flow[ByteString]
        .via(Framing.delimiter( //chunk the inputs up into actual lines of text
          ByteString("\n"),
          maximumFrameLength = 256,
          allowTruncation = true))
        .map(_.utf8String)
        .via(commandParser)
        .merge(welcomeSource) // merge the initial banner after parser
        .map(_ + "\n")
        .map(ByteString(_))
        .watchTermination()((_, done) => done.onComplete {
        case Failure(err) =>
          println(s"Server flow failed: $err")
        case _ => println(s"Server flow terminated for client: ${connection.remoteAddress}")
      })
      connection.handleWith(serverEchoFlow)
    }
    
    val connections = Tcp().bind(interface = address, port = port)
    val binding = connections.watchTermination()(Keep.left).to(handler).run()

    binding.onComplete {
      case Success(b) =>
        println("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        println(s"Server could not bind to: $address:$port: ${e.getMessage}")
        system.terminate()
    }
    binding
  }

  def client(id: Int, system: ActorSystem, address: String, port: Int): Unit = {
    implicit val sys = system
    implicit val ec = system.dispatcher
    implicit val materializer = ActorMaterializer()

    val connection: Flow[ByteString, ByteString, Future[Tcp.OutgoingConnection]] = Tcp().outgoingConnection(address, port)
    val testInput = ('a' to 'z').map(ByteString(_)) ++ Seq(ByteString("BYE"))
    val source =  Source(testInput).via(connection)
    val closed = source.runForeach(each => println(s"Client: $id received echo: ${each.utf8String}"))
    closed.onComplete(each => println(s"Client: $id closed: $each"))
  }
}

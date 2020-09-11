package sample.stream

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Framing, Keep, RestartSource, Sink, Source, Tcp}
import akka.util.ByteString
import akka.{Done, NotUsed}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

/**
  * Inspired by:
  * https://doc.akka.io/docs/akka/current/stream/stream-io.html?language=scala
  *
  * Use without parameters to start server and 10 parallel clients.
  *
  * Use parameters `server 0.0.0.0 6001` to start server listening on port 6001.
  *
  * Use parameters `client 127.0.0.1 6001` to start client connecting to
  * server on 127.0.0.1:6001.
  *
  * Start cmd line client:
  * echo -n "Hello World" | nc 127.0.0.1 6000
  *
  */
object TcpEcho extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val systemServer = ActorSystem("TcpEchoServer")
  val systemClient = ActorSystem("TcpEchoClient")

  var serverBinding: Future[Tcp.ServerBinding] = _

    if (args.isEmpty) {
      val (host, port) = ("127.0.0.1", 6000)
      serverBinding = server(systemServer, host, port)
      (1 to 10).par.foreach(each => client(each, systemClient, host, port))
    } else {
      val (host, port) =
        if (args.length == 3) (args(1), args(2).toInt)
        else ("127.0.0.1", 6000)
      if (args(0) == "server") {
        serverBinding = server(systemServer, host, port)
      } else if (args(0) == "client") {
        client(1, systemClient, host, port)
      }
    }

  def server(system: ActorSystem, host: String, port: Int): Future[Tcp.ServerBinding] = {
    implicit val sys = system
    implicit val ec = system.dispatcher

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
          logger.info(s"Server flow failed: $err")
        case _ => logger.info(s"Server flow terminated for client: ${connection.remoteAddress}")
      })
      connection.handleWith(serverEchoFlow)
    }

    val connections = Tcp().bind(interface = host, port = port)
    val binding = connections.watchTermination()(Keep.left).to(handler).run()

    binding.onComplete {
      case Success(b) =>
        logger.info("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        logger.info(s"Server could not bind to: $host:$port: ${e.getMessage}")
        system.terminate()
    }

    binding
  }

  def client(id: Int, system: ActorSystem, address: String, port: Int): Unit = {
    implicit val sys = system
    implicit val ec = system.dispatcher

    val connection: Flow[ByteString, ByteString, Future[Tcp.OutgoingConnection]] = Tcp().outgoingConnection(address, port)
    val testInput = ('a' to 'z').map(ByteString(_)) ++ Seq(ByteString("BYE"))

    val restartSource: Source[ByteString, NotUsed] = RestartSource.onFailuresWithBackoff(
      minBackoff = 1.seconds,
      maxBackoff = 60.seconds,
      randomFactor = 0.2
    ) { () => Source(testInput).via(connection)}
    val closed: Future[Done] = restartSource.runForeach(each => logger.info(s"Client: $id received echo: ${each.utf8String}"))
    closed.onComplete(each => logger.info(s"Client: $id closed: $each"))
  }
}

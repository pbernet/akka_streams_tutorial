package akka.grpc.echo

import akka.NotUsed
import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.grpc.echo.gen._
import akka.http.scaladsl.Http
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Source
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.parallel.CollectionConverters._
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

/**
  * A simple compact chat system based on gRPC in the same
  * fashion as [[akkahttp.WebsocketChatEcho]]
  * For simplicity reasons all the grpc sources are kept in one place
  *
  * Doc:
  * https://doc.akka.io/docs/akka-grpc/current/server/walkthrough.html
  * https://developer.lightbend.com/guides/akka-grpc-quickstart-scala
  *
  */
object GrpcChatEcho extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val systemServer = ActorSystem("GrpcChatEchoServer")
  val systemClient = ActorSystem("GrpcChatEchoClient")

  val (host, port) = ("127.0.0.1", 8080)

  server(systemServer, host, port)
  (1 to 2).par.foreach(each => client(each, systemClient, host, port))

  def server(system: ActorSystem, host: String, port: Int) = {
    implicit val sys = system
    implicit val ec = system.dispatcher

    val service = GreeterServiceHandler(new GreeterServiceImpl())

    val bound: Future[Http.ServerBinding] = Http(system)
      .newServerAt(host, port)
      .bind(service)
      .map(_.addToCoordinatedShutdown(hardTerminationDeadline = 10.seconds))

    bound.onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        logger.info(s"gRPC server bound to: ${address.getHostString}:${address.getPort}")
      case Failure(ex) =>
        logger.info("Failed to bind gRPC endpoint, terminating system", ex)
        system.terminate()
    }
  }


  def client(id: Int, system: ActorSystem, host: String, port: Int) = {
    implicit val sys = system
    implicit val ec = system.dispatcher

    val clientSettings = GrpcClientSettings
      .connectToServiceAt(host, port)
      .withTls(false)

    val greeterServiceClient = GreeterServiceClient(clientSettings)

    def runStreamingRequestReplyExample(id: Int): Unit = {
      logger.info(s"Started client: $id")

      val requestStream: Source[HelloRequest, NotUsed] =
        Source(1 to 100)
          .throttle(1, 1.second, 10, ThrottleMode.shaping)
          .map(i => HelloRequest(s"Alice-$id-$i", id))

      val responseStream: Source[HelloReply, NotUsed] = greeterServiceClient.streamHellos(requestStream)

      val done = responseStream
        .runForeach(reply => logger.info(s"Client: $id got streaming reply: ${reply.message}"))

      done.onComplete {
        case Success(_) =>
          logger.info("streamingRequestReply done")
        case Failure(e) =>
          logger.info(s"Error streamingRequestReply: $e")
      }
    }

    runStreamingRequestReplyExample(id)
  }
}

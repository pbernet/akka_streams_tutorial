package akka.grpc.echo

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.http.scaladsl.Http
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.parallel.CollectionConverters._
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

/**
  * Roundtrip using gRPC showing:
  *  - runSingleRequestReply
  *
  *
  * Doc:
  * https://doc.akka.io/docs/akka-grpc/current/server/walkthrough.html
  * https://developer.lightbend.com/guides/akka-grpc-quickstart-scala
  *
  * TODO
  *  - Add access to Metadata
  *
  */
object GrpcEcho extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val systemServer = ActorSystem("GrpcEchoServer")
  val systemClient = ActorSystem("GrpcEchoClient")

  val (host, port) = ("127.0.0.1", 8080)
  server(systemServer, host, port)
  (1 to 1).par.foreach(each => client(each, systemClient, host, port))

  def server(system: ActorSystem, host: String, port: Int) = {
    implicit val sys = system
    implicit val ec = system.dispatcher

    // For serving multiple services, see:
    // https://doc.akka.io/docs/akka-grpc/current/server/walkthrough.html#serving-multiple-services
    val service = GreeterServiceHandler(new GreeterServiceImpl())

    val bound: Future[Http.ServerBinding] = Http(system)
      .newServerAt(host, port)
      // TODO Enable HTTPS with dummy context:
      // https://github.com/akka/akka-http/blob/master/akka-http-core/src/test/scala/akka/http/impl/util/ExampleHttpContexts.scala
      //.enableHttps(serverHttpContext)
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

    // Configure the client by code:
    val clientSettings = GrpcClientSettings
      .connectToServiceAt(host, port)
      //.withDeadline(1.second) //TODO Check impact and need
      .withTls(false)


    // TODO Maybe add discovery
    // Or via application.conf:
    // And via service discovery https://doc.akka.io/docs/akka-grpc/current/client/configuration.html#using-akka-discovery-for-endpoint-discovery
    // val clientSettings = GrpcClientSettings.fromConfig(GreeterService.name)

    // Create a client-side stub for the service
    val greeterServiceClient = GreeterServiceClient(clientSettings)

    // Run examples for each of the exposed service methods.
    //    runSingleRequestReplyExample()
    //    runStreamingRequestExample()
    //    runStreamingReplyExample()
    //    runStreamingRequestReplyExample()

    system.scheduler.scheduleAtFixedRate(1.second, 1.second)(() => runSingleRequestReplyExample(id))

    def runSingleRequestReplyExample(id: Int): Unit = {
      logger.info(s"Client: $id request")
      val reply = greeterServiceClient.sayHello(HelloRequest("Alice", id))
      reply.onComplete {
        case Success(msg) =>
          logger.info(s"Client: $id got single reply: $msg")
        case Failure(e) =>
          logger.info(s"Client: $id error sayHello: $e")
      }
    }

    def runStreamingRequestExample(): Unit = {
      val requests = List("Alice", "Bob", "Peter").map(HelloRequest(_))
      val reply = greeterServiceClient.itKeepsTalking(Source(requests))
      reply.onComplete {
        case Success(msg) =>
          println(s"got single reply for streaming requests: $msg")
        case Failure(e) =>
          println(s"Error streamingRequest: $e")
      }
    }

    def runStreamingReplyExample(): Unit = {
      val responseStream = greeterServiceClient.itKeepsReplying(HelloRequest("Alice"))
      val done: Future[Done] =
        responseStream.runForeach(reply => println(s"got streaming reply: ${reply.message}"))

      done.onComplete {
        case Success(_) =>
          println("streamingReply done")
        case Failure(e) =>
          println(s"Error streamingReply: $e")
      }
    }

    def runStreamingRequestReplyExample(): Unit = {
      val requestStream: Source[HelloRequest, NotUsed] =
        Source
          .tick(100.millis, 1.second, "tick")
          .zipWithIndex
          .map { case (_, i) => i }
          .map(i => HelloRequest(s"Alice-$i"))
          .take(10)
          .mapMaterializedValue(_ => NotUsed)

      val responseStream: Source[HelloReply, NotUsed] = greeterServiceClient.streamHellos(requestStream)
      val done: Future[Done] =
        responseStream.runForeach(reply => println(s"got streaming reply: ${reply.message}"))

      done.onComplete {
        case Success(_) =>
          println("streamingRequestReply done")
        case Failure(e) =>
          println(s"Error streamingRequestReply: $e")
      }
    }


  }


}

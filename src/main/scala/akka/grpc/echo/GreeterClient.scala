package akka.grpc.echo

import akka.Done
import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.grpc.echo.gen._
import akka.stream.scaladsl.Source
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}


/**
  * gRPC client to run against [[GreeterServer]]
  * For simplicity reasons all the grpc sources are kept in one place
  *
  * TODO
  * - refactor retry
  * - add // clients?
  * - Add Balancing
  * - Add access to Metadata?
  */
object GreeterClient extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("GreeterClient")
  implicit val executionContext = system.dispatcher
  
  val clientSettings = GrpcClientSettings
    .connectToServiceAt("127.0.0.1", 8080)
    //.withDeadline(1.second) //TODO Check impact and need
    .withTls(false)


  // TODO Maybe add discovery
  // Or via application.conf:
  // And via service discovery https://doc.akka.io/docs/akka-grpc/current/client/configuration.html#using-akka-discovery-for-endpoint-discovery
  // val clientSettings = GrpcClientSettings.fromConfig(GreeterService.name)

  // Create a client-side stub for the service
  val client: GreeterService = GreeterServiceClient(clientSettings)


  def runSingleRequestReplyExample(id: Int): Unit = {

    // Use akka retry utility to handle case when server not reachable one first request
    implicit val scheduler = system.scheduler
    val maxAttempts = 5
    val delay = 1

    val retried = akka.pattern.retry[HelloReply](
      attempt = () => client.sayHello(HelloRequest("Alice", id)),
      attempts = maxAttempts,
      delay = delay.second)

    retried.onComplete {
          case Success(msg) =>
            logger.info(s"Client: $id got single reply: $msg")
          case Failure(e) =>
            logger.info(s"Server not reachable after: $maxAttempts attempts within ${maxAttempts*delay} seconds. Reply from server: $e")
        }
  }

  def runStreamingRequestExample(): Unit = {
    val requests = List("Alice", "Bob", "Peter").map(HelloRequest(_))
    val reply = client.itKeepsTalking(Source(requests))
    reply.onComplete {
      case Success(msg) =>
        logger.info(s"got single reply for streaming requests: $msg")
      case Failure(e) =>
        logger.info(s"Error streamingRequest: $e")
    }
  }

  def runStreamingReplyExample(): Unit = {
    val responseStream = client.itKeepsReplying(HelloRequest("Alice"))
    val done: Future[Done] =
      responseStream.runForeach(reply => logger.info(s"got streaming reply: ${reply.message}"))

    done.onComplete {
      case Success(_) =>
        logger.info("streamingReply done")
      case Failure(e) =>
        logger.info(s"Error streamingReply: $e")
    }
  }


  // Run examples for each of the exposed service methods
  system.scheduler.scheduleAtFixedRate(1.second, 1.second)(() => runSingleRequestReplyExample(1))
  //runStreamingRequestExample()
  //runStreamingReplyExample()
}

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
    // Time to wait for a reply from server and retry our request after that
    .withDeadline(1.second)
    .withTls(false)


  // TODO Maybe add discovery
  // Or via application.conf:
  // And via service discovery https://doc.akka.io/docs/akka-grpc/current/client/configuration.html#using-akka-discovery-for-endpoint-discovery
  // val clientSettings = GrpcClientSettings.fromConfig(GreeterService.name)

  val client: GreeterService = GreeterServiceClient(clientSettings)


  def runSingleRequestReplyExample(id: Int): Unit = {
      withRetry(() => client.sayHello(HelloRequest("Alice", id)), id)
  }

  def runStreamingRequestExample(id: Int): Unit = {
    val source = Source(1 to 100)
      .map(each => HelloRequest(each.toString, id))
    withRetry(() => client.itKeepsTalking(source), id)
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

  // Use akka retry utility to handle case when server is not reachable on initial request
  private def withRetry(fun: () => Future[HelloReply], id: Int) = {
    implicit val scheduler = system.scheduler
    val maxAttempts = 10
    val delay = 1

    val retried = akka.pattern.retry[HelloReply](
      attempt = fun,
      attempts = maxAttempts,
      delay = delay.second)

    retried.onComplete {
      case Success(msg) =>
        logger.info(s"Client: $id got reply: $msg")
      case Failure(e) =>
        logger.info(s"Server not reachable on initial request after: $maxAttempts attempts within ${maxAttempts*delay} seconds. Give up. Ex: $e")
    }
  }


  // Run examples for each of the exposed service methods
  //system.scheduler.scheduleAtFixedRate(1.second, 1.second)(() => runSingleRequestReplyExample(1))
  runStreamingRequestExample(1)
  //runStreamingReplyExample()
}

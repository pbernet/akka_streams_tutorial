package akka.grpc.echo

import akka.Done
import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.grpc.echo.gen._
import akka.stream.RestartSettings
import akka.stream.scaladsl.{RestartSource, Sink, Source}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.parallel.CollectionConverters._
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}


/**
  * gRPC client to run against [[GreeterServer]]
  * Playground to test different ways of interaction, with a focus on resilience
  *
  */
object GreeterClient extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("GreeterClient")
  implicit val executionContext = system.dispatcher

  val maxElements = 10

  // For all requests
  // set config:
  // https://doc.akka.io/docs/akka-grpc/current/client/configuration.html#using-akka-discovery-for-endpoint-discovery
  // and metadata:
  // https://doc.akka.io/docs/akka-grpc/current/client/details.html#request-metadata
  val clientSettings = GrpcClientSettings
    .connectToServiceAt("127.0.0.1", 8080)
    .withTls(false)

  val client: GreeterService = GreeterServiceClient(clientSettings)


  // Comment out to run examples in isolation
  (1 to 1).par.foreach(each => runSingleRequestReplyExample(each))
  (1 to 1).par.foreach(each => runStreamingRequestExample(each))
  (1 to 1).par.foreach(each => runStreamingReplyExample(each))


  // Send single message and wait for ACK (to get message serialisation). Retry on failure
  def runSingleRequestReplyExample(id: Int): Unit = {

    def sendAndReceive(i: Int): Future[Done] = {
      Source.single(id)
        .mapAsync(1)(each => client.sayHello(HelloRequest(s"$each-$i", id)))
        .runForeach(reply => logger.info(s"Client: $id received single reply: $reply"))
        .recoverWith {
          case ex: RuntimeException =>
            logger.warn(s"Client: $id about to retry for element $i, because of: $ex")
            Thread.sleep(1000)
            sendAndReceive(i)
          case e: Throwable => Future.failed(e)
        }
    }

    Source(1 to maxElements)
      .throttle(1, 1.second)
      .mapAsync(1)(i => sendAndReceive(i))
      .runWith(Sink.ignore)
  }

  // Send a stream of messages and wait for ACK after last message. Retry all on failure
  def runStreamingRequestExample(id: Int): Unit = {
    val source = Source(1 to maxElements)
      .throttle(1, 1.second)
      .wireTap(each => logger.info(s"Client: $id sending streamed msg: $each/$maxElements"))
      .map(each => HelloRequest(s"$id-$each/$maxElements", id))
    withRetry(() => client.itKeepsTalking(source), id)
  }


  def runStreamingReplyExample(id: Int): Unit = {
    val restartSettings = RestartSettings(1.second, 10.seconds, 0.2).withMaxRestarts(10, 1.minute)
    val restartSource = RestartSource.withBackoff(restartSettings) { () =>
      client.itKeepsReplying(HelloRequest("1", id))
    }

    val done = restartSource
      .runForeach(reply => logger.info(s"Client: $id got streaming reply: ${reply.timestamp}"))

    done.onComplete {
      case Success(_) =>
        logger.info("streamingReply done")
      case Failure(e) =>
        logger.info(s"Error streamingReply: $e")
    }
  }

  // Use akka retry to handle case when server is not reachable
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
        logger.info(s"Client: $id received streamed reply: $msg")
      case Failure(e) =>
        logger.info(s"Server not reachable on initial request after: $maxAttempts attempts within ${maxAttempts*delay} seconds. Give up. Ex: $e")
    }
  }
}

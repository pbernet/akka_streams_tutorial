package alpakka.sqs

import akka.actor.ActorSystem
import akka.stream.alpakka.sqs.scaladsl.{SqsAckSink, SqsPublishSink, SqsSource}
import akka.stream.alpakka.sqs._
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import org.apache.commons.validator.routines.UrlValidator
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{CreateQueueRequest, DeleteQueueRequest, DeleteQueueResponse, Message}

import java.net.URI
import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration.{DurationInt, SECONDS}

/**
  * Echo flow via a SQS standard queue:
  *  - produce n String msgs
  *  - consume n String msgs
  *
  * Run this class against your AWS account using hardcoded accessKey/secretKey
  * or
  * Run via [[alpakka.sqs.SqsEchoIT]] against localStack docker container
  *
  * Remarks:
  * - For convenience we use the async `awsSqsClient` to create/delete the queue
  * - Be warned that running against AWS (and thus setting up resources) can cost you money
  *
  * Doc:
  * https://doc.akka.io/docs/alpakka/current/sqs.html
  * https://docs.localstack.cloud/user-guide/aws/sqs
  */
class SqsEcho(urlWithMappedPort: URI = new URI(""), accessKey: String = "", secretKey: String = "", region: String = "") {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("SqsEcho")
  implicit val executionContext = system.dispatcher

  val queueName = "mysqs-queue"
  private var queueUrl = ""

  implicit val awsSqsClient =
    if (new UrlValidator().isValid(urlWithMappedPort.toString)) {
      logger.info("Running against localStack on local container...")
      val credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))

      SqsAsyncClient
        .builder()
        .endpointOverride(urlWithMappedPort)
        .credentialsProvider(credentialsProvider)
        .region(Region.of(region))
        .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
        .build()
    }
    else {
      logger.info("Running against AWS...")
      // Add your credentials
      val credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("***", "***"))
      SqsAsyncClient
        .builder()
        .credentialsProvider(credentialsProvider)
        .region(Region.EU_WEST_1)
        .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
        .build()
    }
  system.registerOnTermination(awsSqsClient.close())

  def run(): Int = {
    queueUrl = createStandardQueue()
    logger.info(s"Created queue with URL: $queueUrl")

    val done = for {
      _ <- producerClient()
      consumed <- consumerClient()
    } yield consumed

    val result = Await.result(done, 10.seconds)
    logger.info(s"Successfully consumed: ${result.size} msgs")
    result.size
  }

  private def producerClient() = {
    logger.info(s"About to start producing msgs to URL: {}", queueUrl)

    val messages = for (i <- 0 until 10) yield s"Message - $i"

    val done = Source(messages)
      .runWith(SqsPublishSink.grouped(queueUrl, SqsPublishGroupedSettings.Defaults.withMaxBatchSize(2)))
    done
  }

  private def consumerClient() = {
    logger.info(s"About to start consuming msgs from URL: {}", queueUrl)
    // Grace time to process msgs on server
    Thread.sleep(1000)

    val settings = SqsSourceSettings()
      // Long polling to avoid (expensive) empty reads, MAX is 20s
      // https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html
      .withWaitTime(2.seconds)
      .withMaxBufferSize(100)
      .withMaxBatchSize(2)
      .withAttributes(immutable.Seq(SenderId, SentTimestamp))
      // Let the stream complete when there are no more messages on the queue
      // In realistic scenarios, you should add a KillSwitch to the stream
      .withCloseOnEmptyReceive(true)
      // Invisible for other concurrent consumers
      // https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
      .withVisibilityTimeout(10.seconds)

    val ackFlow = Flow[Message]
      .map(MessageAction.Delete(_))
      .to(SqsAckSink(queueUrl)) // Handle ack/deletion (via internal ReceiptHandle)

    val messages =
      SqsSource(queueUrl, settings)
        .alsoTo(ackFlow)
        .wireTap(msg => logger.info(s"Received msg: $msg"))
        .runWith(Sink.seq)
    messages
  }

  // When the queue already exists, return it's queueUrl
  private def createStandardQueue(): String = {
    val response = awsSqsClient
      .createQueue(
        CreateQueueRequest.builder()
          .queueName(queueName)
          .build())
      .get(10, SECONDS)
    response.queueUrl()
  }

  private def deleteQueue(): DeleteQueueResponse = {
    val response = awsSqsClient
      .deleteQueue(DeleteQueueRequest.builder()
        .queueUrl(queueUrl)
        .build())
      .get(10, SECONDS)
    logger.info("Successfully deleted SQS queue with URL: {}", queueUrl)
    response
  }
}

object SqsEcho extends App {
  val echo = new SqsEcho()
  echo.run()
  // Avoid dangling resources on AWS, comment out for testing
  echo.deleteQueue()
}

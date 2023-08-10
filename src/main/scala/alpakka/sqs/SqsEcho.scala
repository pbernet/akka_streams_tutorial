package alpakka.sqs

import akka.actor.ActorSystem
import akka.stream.alpakka.sqs.scaladsl.{SqsPublishSink, SqsSource}
import akka.stream.alpakka.sqs.{SqsPublishGroupedSettings, SqsSourceSettings}
import akka.stream.scaladsl.{Sink, Source}
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import org.apache.commons.validator.routines.UrlValidator
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{CreateQueueRequest, CreateQueueResponse, DeleteQueueRequest, DeleteQueueResponse}

import java.net.URI
import scala.concurrent.Await
import scala.concurrent.duration.{DurationInt, SECONDS}

/**
  * Echo flow via a SQS queue:
  *  - upload n String msgs
  *  - download n String msgs
  *
  * Run this class against your AWS account using hardcoded accessKey/secretKey
  * or
  * Run via [[alpakka.sqs.SqsEchoIT]] against localstack docker container
  *
  * Remarks:
  * - For convenience we use the async `awsSqsClient` to create/delete the queue
  * - Be warned that running against AWS (and thus setting up resources) can cost you money
  *
  * Doc:
  * https://doc.akka.io/docs/alpakka/current/sqs.html
  * https://docs.localstack.cloud/user-guide/aws/sqs/
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
    queueUrl = createQueue()
    logger.info(s"Created queue with URL: $queueUrl")

    val done = for {
      _ <- producerClient()
      consumed <- consumerClient()
    } yield consumed

    val result = Await.result(done, 10.seconds)
    logger.info(s"Successfully downloaded: ${result.size} records")
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
    logger.info(s"About to start consuming from URL: {}", queueUrl)
    // Grace time to process msgs on server
    Thread.sleep(1000)

    val messages =
      SqsSource(
        queueUrl,
        SqsSourceSettings().withCloseOnEmptyReceive(true).withWaitTime(10.millis)
      ).runWith(Sink.seq)
    messages
  }

  // When the queue already exists, return it's queueUrl
  private def createQueue() = {
    val response: CreateQueueResponse = awsSqsClient
      .createQueue(
        CreateQueueRequest.builder()
          .queueName(queueName)
          .build())
      .get(10, SECONDS)
    response.queueUrl()
  }

  private def deleteQueue() = {
    val response: DeleteQueueResponse = awsSqsClient
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
  // Avoid dangling resources on AWS
  echo.deleteQueue()
}

package alpakka.kinesis

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.kinesis.scaladsl.{KinesisFlow, KinesisSource}
import akka.stream.alpakka.kinesis.{KinesisFlowSettings, ShardIterator, ShardSettings}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import org.apache.commons.validator.routines.UrlValidator
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.{PutRecordsRequestEntry, PutRecordsResultEntry, Record}

import java.net.URI
import java.nio.charset.StandardCharsets
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

/**
  * Echo flow via a Kinesis "provisioned data stream" with 1 shard:
  *  - upload n binary elements
  *  - download n binary elements
  *
  * Run this class against your AWS account using hardcoded accessKey/secretKey
  * Prerequisite: Create a "provisioned data stream" with 1 shard on AWS console
  * or
  * Run via [[alpakka.kinesis.KinesisEchoIT]] against localStack docker container
  *
  * Remarks:
  *  - No computation on the server side, just echo routing
  *  - Default data retention time is 24h, hence with the setting `TrimHorizon` we may receive old records...
  *  - A Kinesis data stream with 1 shard should preserve order, however duplicates may occur due to retries
  *  - Be warned that running against AWS (and thus setting up resources) can cost you money
  *
  * Doc:
  * https://doc.akka.io/docs/alpakka/current/kinesis.html
  */
class KinesisEcho(urlWithMappedPort: URI = new URI(""), accessKey: String = "", secretKey: String = "", region: String = "") {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("KinesisEcho")
  implicit val executionContext = system.dispatcher

  val kinesisDataStreamName = "kinesisDataStreamProvisioned"
  val shardIdName = "shardId-000000000000"

  val batchSize = 10

  implicit val awsKinesisClient: KinesisAsyncClient = {
    if (new UrlValidator().isValid(urlWithMappedPort.toString)) {
      logger.info("Running against localStack on local container...")
      val credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))

      KinesisAsyncClient
        .builder()
        .endpointOverride(urlWithMappedPort)
        .credentialsProvider(credentialsProvider)
        .region(Region.of(region))
        .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
        // Possible to configure retry policy
        // see https://doc.akka.io/docs/alpakka/current/aws-shared-configuration.html
        // .overrideConfiguration(...)
        .build()
    } else {
      logger.info("Running against AWS...")
      // For now use hardcoded credentials
      val credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("***", "***"))

      KinesisAsyncClient
        .builder()
        .credentialsProvider(credentialsProvider)
        .region(Region.EU_WEST_1)
        .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
        // Possible to configure retry policy
        // see https://doc.akka.io/docs/alpakka/current/aws-shared-configuration.html
        // .overrideConfiguration(...)
        .build()
    }
  }
  system.registerOnTermination(awsKinesisClient.close())

  def run(): Int = {

    val done = for {
      _ <- producerClient()
      consumed <- consumerClient()
    } yield consumed

    val result: Seq[String] = Await.result(done, 60.seconds)
    logger.info(s"Successfully downloaded: ${result.size} records")
    terminateWhen(done)
    result.size
  }

  private def producerClient() = {
    logger.info(s"About to start upload...")

    val defaultFlowSettings = KinesisFlowSettings.Defaults
    val kinesisFlow: Flow[PutRecordsRequestEntry, PutRecordsResultEntry, NotUsed] = KinesisFlow(kinesisDataStreamName, defaultFlowSettings)

    val done: Future[Seq[PutRecordsResultEntry]] = Source(1 to batchSize)
      .map(each => convertToRecord(each))
      .via(kinesisFlow)
      .runWith(Sink.seq)

    done.onComplete(result => logger.info(s"Successfully uploaded: ${result.get.size} records"))
    done
  }

  private def consumerClient(): Future[Seq[String]] = {
    logger.info(s"About to start download...")

    val settings = {
      ShardSettings(streamName = kinesisDataStreamName, shardId = shardIdName)
        .withRefreshInterval(1.second)
        .withLimit(500)
        // TrimHorizon: same as "earliest" in Kafka
        .withShardIterator(ShardIterator.TrimHorizon)
    }

    val source: Source[software.amazon.awssdk.services.kinesis.model.Record, NotUsed] =
      KinesisSource.basic(settings, awsKinesisClient)

    source
      .map(each => convertToString(each))
      .wireTap(each => logger.debug(s"Downloaded: $each"))
      .take(batchSize)
      .runWith(Sink.seq)
  }


  private def convertToRecord(each: Int) = {
    PutRecordsRequestEntry
      .builder()
      .partitionKey(s"partition-key-$each")
      .data(SdkBytes.fromByteBuffer(ByteString(s"data-$each").asByteBuffer))
      .build()
  }

  private def convertToString(record: Record) = {
    val processingTimestamp = record.approximateArrivalTimestamp
    val data = record.data.asString(StandardCharsets.UTF_8)
    s"Processing time: $processingTimestamp. Data:$data"
  }

  private def terminateWhen(done: Future[Seq[String]]): Unit = {
    done.onComplete {
      case Success(_) =>
        logger.info(s"Flow Success. About to terminate...")
        system.terminate()
      case Failure(e) =>
        logger.info(s"Flow Failure: $e. About to terminate...")
        system.terminate()
    }
  }
}

object KinesisEcho extends App {
  val echo = new KinesisEcho()
  echo.run()
  // TODO Add stream removal
}

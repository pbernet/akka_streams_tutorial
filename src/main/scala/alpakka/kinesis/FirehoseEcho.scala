package alpakka.kinesis

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Attributes
import akka.stream.alpakka.kinesisfirehose.scaladsl.KinesisFirehoseFlow
import akka.stream.alpakka.s3.AccessStyle.PathAccessStyle
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.alpakka.s3.{ListBucketResultContents, S3Attributes, S3Settings}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.firehose.FirehoseAsyncClient
import software.amazon.awssdk.services.firehose.model.{PutRecordBatchResponseEntry, Record => RecordFirehose}

import java.net.URI
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}


/**
  * Show the possibilities of a "Firehose pipeline"; eg
  * producerClientFirehose()
  * --> Elasticsearch -> Check entries manually via browserClient()
  * +-> S3            -> Check via countFilesBucket()
  *
  * Run via [[alpakka.firehose.FirehoseEchoIT]] against localStack docker container
  * Possible to run against AWS, after a all the resources are setup via console
  *
  * Doc:
  * https://docs.localstack.cloud/user-guide/aws/kinesis-firehose
  * https://doc.akka.io/docs/alpakka/current/kinesis.html
  */
class FirehoseEcho(urlWithMappedPort: URI = new URI("http://localhost:4566"), accessKey: String = "accessKey", secretKey: String = "secretKey", region: String = "us-east-1") {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("FirehoseEcho")
  implicit val executionContext = system.dispatcher

  val firehoseStreamName = "activity-to-elasticsearch-local"
  val s3BucketName = "kinesis-activity-backup-local"

  val batchSize = 10

  val credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))

  private val s3Settings: S3Settings =
    S3Settings()
      .withAccessStyle(PathAccessStyle)
      .withEndpointUrl(urlWithMappedPort.toString)
      .withCredentialsProvider(credentialsProvider)

  implicit private val s3attributes: Attributes = S3Attributes.settings(s3Settings)

  implicit val awsFirehoseClient: FirehoseAsyncClient = {
    FirehoseAsyncClient
      .builder()
      .endpointOverride(urlWithMappedPort)
      .credentialsProvider(credentialsProvider)
      .region(Region.of(region))
      .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
      .build()
  }
  system.registerOnTermination(awsFirehoseClient.close())

  def run() = {
    val done = for {
      _ <- producerClientFirehose()
      filesFut <- countFilesBucket()
    } yield filesFut

    val result = Await.result(done, 80.seconds)
    result.size
  }

  private def producerClientFirehose() = {
    logger.info(s"About to start Firehose upload...")
    val firehoseFlow: Flow[RecordFirehose, PutRecordBatchResponseEntry, NotUsed] = KinesisFirehoseFlow(firehoseStreamName)

    val done = Source(1 to batchSize)
      .map(each => convertToBatchRecord(each))
      .via(firehoseFlow)
      .runWith(Sink.seq)

    done.onComplete(result => logger.info(s"Successfully uploaded: ${result.get.size} records"))
    done
  }

  private def convertToBatchRecord(each: Int): RecordFirehose = {
    val payload = s"{ \"target\": \"myTarget_$each\" }"
    RecordFirehose.builder().data(SdkBytes.fromByteBuffer(ByteString(payload).asByteBuffer)).build()
  }

  private def countFilesBucket() = {
    val resultFut: Future[Seq[ListBucketResultContents]] = S3
      .listBucket(s3BucketName, None)
      .withAttributes(s3attributes)
      .runWith(Sink.seq)

    resultFut.onComplete(result => logger.info(s"Number of files in bucket: ${result.get.size}"))
    resultFut
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

object FirehoseEcho extends App {
  // Use to connect to localStack with default params, eg when localStack image is run via Cockpit
  val echo = new FirehoseEcho()
  echo.run()
}

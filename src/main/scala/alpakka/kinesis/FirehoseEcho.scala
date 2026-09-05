package alpakka.kinesis

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.connectors.awsspi.PekkoHttpClient
import org.apache.pekko.stream.connectors.kinesisfirehose.scaladsl.KinesisFirehoseFlow
import org.apache.pekko.stream.connectors.s3.AccessStyle.PathAccessStyle
import org.apache.pekko.stream.connectors.s3.scaladsl.S3
import org.apache.pekko.stream.connectors.s3.{ListBucketResultContents, S3Attributes, S3Settings}
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import org.apache.pekko.util.ByteString
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.firehose.FirehoseAsyncClient
import software.amazon.awssdk.services.firehose.model.{PutRecordBatchResponseEntry, Record as RecordFirehose}

import java.net.URI
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future}


/**
  * Show the possibilities of a "Firehose pipeline"; eg
  * producerClientFirehose()
  * +-> S3 -> Check via countFilesBucket()
  *
  * Run via [[alpakka.firehose.FirehoseEchoIT]] against ministack docker container
  * Possible to run against AWS, after all the resources are setup via console
  *
  * Doc:
  * https://ministack.org
  * https://doc.akka.io/docs/alpakka/current/kinesis.html
  */
class FirehoseEcho(urlWithMappedPort: URI = new URI("http://localhost:4566"), accessKey: String = "accessKey", secretKey: String = "secretKey", region: String = "us-east-1") {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem("FirehoseEcho")
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val firehoseStreamName = "activity-to-elasticsearch-local"
  val s3BucketName = "kinesis-activity-backup-local"

  val batchSize = 10

  val credentialsProvider: StaticCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))

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
      .httpClient(PekkoHttpClient.builder().withActorSystem(system).build())
      .build()
  }
  system.registerOnTermination(awsFirehoseClient.close())

  def run(): Int = {
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
}

object FirehoseEcho {
  def main(args: Array[String]): Unit = {
    // Use to connect to ministack with default params, eg when ministack image is run via Cockpit
    val echo = new FirehoseEcho()
    echo.run()
  }
}

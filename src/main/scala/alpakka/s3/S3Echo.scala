package alpakka.s3

import org.apache.commons.io.FileUtils
import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.connectors.file.ArchiveMetadata
import org.apache.pekko.stream.connectors.file.scaladsl.Archive
import org.apache.pekko.stream.connectors.s3.AccessStyle.PathAccessStyle
import org.apache.pekko.stream.connectors.s3._
import org.apache.pekko.stream.connectors.s3.scaladsl.S3
import org.apache.pekko.stream.scaladsl.{FileIO, Keep, Sink, Source}
import org.apache.pekko.util.ByteString
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}

import java.nio.file.{Path, Paths}
import java.util.UUID
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Implement two S3 file upload/download roundtrips, where the 2nd depends on the 1st:
  *  - roundtrip1Client: upload n files   -> download n files
  *  - roundtrip2Client: download n files -> zip n files locally -> upload zip file
  *
  * Run this class against your AWS account using the settings in application.conf
  * or
  * Run via [[alpakka.s3.S3EchoMinioIT]] against local minio docker container
  *
  * Doc:
  * https://doc.akka.io/docs/alpakka/current/s3.html
  */
class S3Echo(urlWithMappedPort: String = "", accessKey: String = "", secretKey: String = "") {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("S3Echo")
  implicit val executionContext = system.dispatcher

  private val resourceFileName = "63MB.pdf"
  private val archiveFileName = "archive.zip"

  // Bucket name must be unique and may only contain certain characters
  // https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
  private val bucketName = s"s3echo-earthling-paul-unique"

  private val s3Settings: S3Settings = if (urlWithMappedPort.isEmpty) {
    S3Ext(system).settings
  } else {
    S3Settings()
      .withAccessStyle(PathAccessStyle)
      .withEndpointUrl(s"http://$urlWithMappedPort")
      .withCredentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))
      )
  }

  implicit private val s3attributes: Attributes = S3Attributes.settings(s3Settings)

  val localTmpDir: Path = Paths.get(System.getProperty("java.io.tmpdir")).resolve(s"s3echo_${UUID.randomUUID().toString.take(4)}")
  logger.info(s"Using localTmpDir dir: $localTmpDir")
  FileUtils.forceMkdir(localTmpDir.toFile)

  def run(): Future[Int] = {

    val doneSequentialProcessing: Future[Int] = for {
      _ <- makeBucket()
      _ <- roundtrip1Client()
      _ <- countFilesBucket()
      _ <- roundtrip2Client()
      nbrOfFilesBucket <- countFilesBucket()
    } yield nbrOfFilesBucket

    terminateWhen(doneSequentialProcessing)
    doneSequentialProcessing
  }

  private def makeBucket() = {
    logger.info(s"Check connection and credentials. Create unique bucket, if it does not already exist: $bucketName")

    S3.checkIfBucketExists(bucketName)
      .flatMap {
        case BucketAccess.NotExists =>
          logger.info(s"Bucket: $bucketName does not exist. About to create bucket...")

          S3.makeBucket(bucketName).flatMap {
            case Done =>
              logger.info(s"Successfully created bucket with name: $bucketName")
              Future.successful(Done)
            case _ =>
              // Hangs, when "endpoint-url" is not correct
              Future.failed(new RuntimeException(s"Unable to create bucket: $bucketName"))
          }
        case BucketAccess.AccessGranted =>
          logger.info(s"Bucket: $bucketName already exists. Proceed...")
          Future.successful(Done)
        case BucketAccess.AccessDenied =>
          Future.failed(new RuntimeException(s"Access denied to create bucket: $bucketName"))
        case resp =>
          Future.failed(new RuntimeException(s"Error during initialization of: $bucketName. Details: ${resp.toString}"))
      }
  }

  private def roundtrip1Client() = {
    logger.info(s"About to start 1st roundtrip...")

    Source(1 to 10)
      .mapAsync(5)(each => uploadClient(each))
      .map(key => downloadClient(key))
      .runWith(Sink.ignore)
  }

  private def uploadClient(id: Int) = {
    val bucketKey = s"${id}_${UUID.randomUUID().toString.take(4)}_$resourceFileName"
    logger.info(s"About to upload file with bucketKey: $bucketKey")

    val fileSource =
      FileIO.fromPath(Paths.get(s"src/main/resources/$resourceFileName"), 1024)

    val s3Sink: Sink[ByteString, Future[MultipartUploadResult]] =
      S3.multipartUpload(bucketName, bucketKey).withAttributes(s3attributes)

    val result: Future[MultipartUploadResult] =
      fileSource.runWith(s3Sink)

    result.flatMap { bucket =>
      logger.info(s"Upload completed: $bucket")
      Future(bucket.key)
    }
  }

  private def downloadClient(bucketKey: String) = {
    logger.info(s"About to download file with bucketKey: $bucketKey")
    // Give AWS time to fully process the previous upload
    Thread.sleep(1000)

    val (metadataFuture, dataFuture) = getObject(bucketKey)

    dataFuture.flatMap { data =>
      val source = Source(data)
      val tmpPath = localTmpDir.resolve(bucketKey)
      val sink = FileIO.toPath(tmpPath)
      logger.info(s"About to write to tmp file: $tmpPath")
      source.runWith(sink)
    }
  }

  private def countFilesBucket() = {
    logger.info(s"About to count files in bucket...")
    val resultFut: Future[Seq[ListBucketResultContents]] = S3
      .listBucket(bucketName, None)
      .withAttributes(s3attributes)
      .runWith(Sink.seq)

    resultFut.flatMap { result =>
      val size = result.size
      logger.info(s"Number of files in bucket: $size")
      Future(size)
    }

  }

  // requires that files are present in s3 bucket (eg from 1st roundtrip)
  private def roundtrip2Client() = {
    logger.info(s"About to start 2nd roundtrip...")
    val s3Sink = S3.multipartUpload(bucketName, archiveFileName).withAttributes(s3attributes)

    S3
      .listBucket(bucketName, None)
      .withAttributes(s3attributes)
      .mapAsync(5) { resContents =>
        val (metadataFuture, dataFuture) = getObject(resContents.key)
        dataFuture.map(seqbs => (ArchiveMetadata(resContents.key), Source(seqbs)))
      }
      .via(Archive.zip())
      .runWith(s3Sink)
  }

  private def getObject(bucketKey: String) = {
    S3
      .getObject(bucketName, bucketKey)
      .withAttributes(s3attributes)
      // We want the content of the whole file, hence Sink.seq
      .toMat(Sink.seq)(Keep.both).run()
  }

  private def terminateWhen(done: Future[Int]): Unit = {
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

object S3Echo extends App {
  new S3Echo().run()
}
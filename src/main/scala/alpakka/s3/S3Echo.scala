package alpakka.s3

import akka.Done
import akka.actor.ActorSystem
import akka.stream.Attributes
import akka.stream.alpakka.file.ArchiveMetadata
import akka.stream.alpakka.file.scaladsl.Archive
import akka.stream.alpakka.s3.AccessStyle.PathAccessStyle
import akka.stream.alpakka.s3._
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.{FileIO, Keep, Sink, Source}
import akka.util.ByteString
import org.apache.commons.io.FileUtils
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}

import java.nio.file.{Path, Paths}
import java.util.UUID
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * S3 file upload/download roundtrips:
  *  - roundtrip1Client: upload n files -> download n files
  *  - roundtrip2Client: download n files -> zip locally -> upload zip file
  *
  * Run this class against your AWS account using the settings in application.conf
  * or
  * Run via [[alpakka.s3.S3EchoMinioIT]] against local minio docker container
  *
  * Doc:
  * https://doc.akka.io/docs/alpakka/current/s3.html
  */
class S3Echo(urlWithMappedPort: String, accessKey: String, secretKey: String) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("S3Echo")
  implicit val executionContext = system.dispatcher

  private val resourceFileName = "testfile.jpg"
  private val archiveFileName = "archive.zip"

  // Bucket name must be unique and may only contain certain characters
  // https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
  private val bucketName = s"s3echo-earthling-paul-unique"

  private val s3Settings: S3Settings = if (urlWithMappedPort.isEmpty) {
    S3Ext(system).settings
  } else {
    S3Settings()
      .withAccessStyle(PathAccessStyle)
      .withEndpointUrl(s"https://$urlWithMappedPort")
      .withCredentialsProvider(
        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))
      )
  }

  implicit private val s3attributes: Attributes = S3Attributes.settings(s3Settings)

  val localTmpDir: Path = Paths.get(System.getProperty("java.io.tmpdir")).resolve(s"s3echo_${UUID.randomUUID().toString.take(4)}")
  logger.info(s"Using localTmpDir dir: $localTmpDir")
  FileUtils.forceMkdir(localTmpDir.toFile)

  def run(): Future[Done] = {
    val done = for {
      _ <- makeBucket()
      _ <- roundtrip1Client()
      _ <- countFilesBucket()
      _ <- roundtrip2Client()
      _ <- countFilesBucket()
    } yield Done

    terminateWhen(done)
    done
  }

  private def makeBucket() = {
    logger.info(s"Check connection and credentials. Create unique bucket, if it does not already exist: $bucketName")

    S3.checkIfBucketExists(bucketName)
      .flatMap {
        case BucketAccess.NotExists =>
          logger.info(s"Bucket: $bucketName does not exist. Creating bucket")
          // Hangs, when "endpoint-url" is not correct
          S3.makeBucket(bucketName)
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
      .runForeach(key => downloadClient(key))
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

    result.onComplete(content => logger.info(s"Upload completed: $content"))
    Future(bucketKey)
  }

  private def downloadClient(bucketKey: String) = {
    logger.info(s"About to download file with bucketKey: $bucketKey")
    // We give AWS time to fully process the upload
    Thread.sleep(1000)
    val s3Source: Source[ByteString, Future[ObjectMetadata]] =
      S3.getObject(bucketName, bucketKey).withAttributes(s3attributes)

    val (metadataFuture, dataFuture) = {
      // We want the content of the whole file, hence Sink.seq
      s3Source.toMat(Sink.seq)(Keep.both).run()
    }

    dataFuture.collect { data =>
      val source = Source(data)
      val tmpPath = localTmpDir.resolve(bucketKey)
      val sink = FileIO.toPath(tmpPath)
      logger.info(s"About to write to tmp file: $tmpPath")
      source.runWith(sink)
    }
  }

  private def countFilesBucket() = {
    val resultFut: Future[Seq[ListBucketResultContents]] = S3
      .listBucket(bucketName, None)
      .withAttributes(s3attributes)
      .runWith(Sink.seq)

    resultFut.onComplete(result => logger.info(s"Number of files in bucket: ${result.get.size}"))
    resultFut
  }

  // requires that files are present in bucket
  private def roundtrip2Client() = {
    logger.info(s"About to start 2nd roundtrip...")
    val s3Sink = S3.multipartUpload(bucketName, archiveFileName).withAttributes(s3attributes)

    S3
      .listBucket(bucketName, None)
      .withAttributes(s3attributes)
      .mapAsync(4) { resContents =>
        val (metadataFuture, dataFuture) =
          S3
            .getObject(bucketName, resContents.key)
            .withAttributes(s3attributes)
            .toMat(Sink.seq)(Keep.both).run()
        dataFuture.map { seqbs => (ArchiveMetadata(resContents.key), Source(seqbs)) }
      }
      .via(Archive.zip())
      .runWith(s3Sink)
  }

  private def terminateWhen(done: Future[Done]): Unit = {
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
  new S3Echo("", "", "").run()
}
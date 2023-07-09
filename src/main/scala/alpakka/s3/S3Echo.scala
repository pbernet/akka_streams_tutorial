package alpakka.s3

import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.alpakka.s3.AccessStyle.PathAccessStyle
import akka.stream.alpakka.s3._
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.{FileIO, Keep, Sink, Source}
import akka.util.ByteString
import org.slf4j.{Logger, LoggerFactory}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}

import java.nio.file.Paths
import java.util.UUID
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * S3 file upload/download roundtrip
  *
  * Run this class against your AWS account using the settings in application.conf
  * or
  * Run via [[S3EchoMinioIT]] against local minio docker container
  *
  * Doc:
  * https://doc.akka.io/docs/alpakka/current/s3.html
  *
  */
class S3Echo(urlWithMappedPort: String, accessKey: String, secretKey: String) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("S3Echo")
  implicit val executionContext = system.dispatcher

  val resourceFileName = "testfile.jpg"
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

  implicit val s3attributes = S3Attributes.settings(s3Settings)

  def run() = {
    makeBucket()
      .onComplete {
        case Success(done) =>
          logger.info(s"Successfully created bucket: $done")
          roundtripClient()
        case Failure(s3ex: S3Exception) =>
          if (s3ex.code.equals("BucketAlreadyOwnedByYou")) {
            logger.warn(s"Bucket already exists, proceed...")
            roundtripClient()
          } else {
            logger.warn(s"Bucket failure: $s3ex")
          }
        case Failure(e) =>
          logger.info(s"Bucket failure: $e")
      }
  }

  private def roundtripClient(): Unit = {
    uploadClient(resourceFileName).onComplete {
      case Success(key) =>
        logger.info(s"About to download with bucketKey: $key")
        Thread.sleep(1000)
        downloadClient(key)
      case Failure(e) =>
        logger.info(s"Failure: $e")
    }
  }

  private def makeBucket() = {
    logger.info(s"Check connection and credentials. Create unique bucket, if it does not already exist: $bucketName")

    // Hangs, when "endpoint-url" is not correct
    S3.makeBucket(bucketName)
  }

  private def uploadClient(resourceFileName: String) = {
    val prefix = UUID.randomUUID().toString.take(4) + "_"
    val bucketKey = s"$prefix$resourceFileName"
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
    val s3Source: Source[ByteString, Future[ObjectMetadata]] =
      S3.getObject(bucketName, bucketKey).withAttributes(s3attributes)

    val (metadataFuture, dataFuture) = {
      // We want the content of the whole file, hence Sink.seq
      s3Source.toMat(Sink.seq)(Keep.both).run()
    }

    dataFuture.onComplete {
      case Success(data) =>
        val source = Source(data)
        val tmpPath = Paths.get(System.getProperty("java.io.tmpdir")).resolve(bucketKey)
        val sink = FileIO.toPath(tmpPath)
        logger.info(s"About to write to tmp file: $tmpPath")
        val done = source.runWith(sink)
        terminateWhen(done)
      case Failure(e) =>
        logger.info(s"Download failure: $e")
    }
  }

  private def terminateWhen(done: Future[IOResult]) = {
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
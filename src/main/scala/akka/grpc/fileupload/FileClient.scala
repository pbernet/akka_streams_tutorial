package akka.grpc.fileupload

import akka.NotUsed
import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.grpc.fileupload.gen._
import akka.stream.scaladsl.{Source, StreamConverters}
import org.slf4j.{Logger, LoggerFactory}

import java.io.FileInputStream
import scala.collection.parallel.CollectionConverters._
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}


/**
  * gRPC client to run against [[FileServer]]
  *
  */
object FileClient extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("FileClient")
  implicit val executionContext = system.dispatcher

  val clientSettings = GrpcClientSettings
    .connectToServiceAt("127.0.0.1", 8081)
    .withTls(false)

  val client: FileService = FileServiceClient(clientSettings)

  (1 to 1).par.foreach(each => runUpload(each))

  // Send a stream of chunked file parts and wait for ACK after last message
  def runUpload(id: Int) = {
    val sourceFileName = "63MB"
    val sourceFileType = "pdf"
    val metadataSource = Source.single(new FileUploadRequest().withMetadata(MetaData(sourceFileName, sourceFileType)))
    val sourceFilePath = s"src/main/resources/$sourceFileName.$sourceFileType"
    val fileInputStream = new FileInputStream(sourceFilePath)

    val fileSource: Source[FileUploadRequest, NotUsed] = StreamConverters
      .fromInputStream(() => fileInputStream, chunkSize = 10 * 1024)
      .map(each => new FileUploadRequest().withFile(akka.grpc.fileupload.gen.File(com.google.protobuf.ByteString.copyFrom(each.toByteBuffer))))
      .prepend(metadataSource) // This is the implementation of the suggested protocol
      .mapMaterializedValue(_ => NotUsed)

    withRetry(() => client.upload(fileSource), id)
  }


  // Use akka retry to handle server failure cases, eg when server is not reachable
  private def withRetry(fun: () => Future[FileUploadResponse], id: Int) = {
    implicit val scheduler = system.scheduler
    val maxAttempts = 10
    val delay = 1

    val retried = akka.pattern.retry[FileUploadResponse](
      attempt = fun,
      attempts = maxAttempts,
      delay = delay.second)

    retried.onComplete {
      case Success(response) =>
        logger.info(s"Client: $id received streamingReply: $response")
      case Failure(e) =>
        logger.info(s"Server not reachable on initial request after: $maxAttempts attempts within ${maxAttempts * delay} seconds. Give up. Ex: $e")
    }
  }
}


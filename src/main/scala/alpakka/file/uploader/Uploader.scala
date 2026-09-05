package alpakka.file.uploader

import org.apache.pekko.actor.{ActorSystem, Terminated}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.marshalling.Marshal
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import org.apache.pekko.http.scaladsl.server.Directives.{complete, logRequestResult, path, *}
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.server.directives.FileInfo
import org.apache.pekko.http.scaladsl.settings.ConnectionPoolSettings
import org.apache.pekko.stream.scaladsl.FileIO
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.nio.file.Files
import scala.compiletime.uninitialized
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

/**
  * HTTP file upload to the embedded mock server
  * Is used by [[DirectoryWatcher]]
  *
  */
class Uploader(system: ActorSystem) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val systemImpl: ActorSystem = system
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  var serverBinding: Future[Http.ServerBinding] = uninitialized

  val (protocol, address, port) = ("http", "localhost", 6010)

  server(address, port)

  def server(address: String, port: Int): Unit = {

    def routes: Route = logRequestResult("uploader") {
      path("api" / "upload") {

        def tempDestination(fileInfo: FileInfo): File = File.createTempFile(fileInfo.fileName, ".tmp.server")

        storeUploadedFile("uploadedFile", tempDestination) {
          case (metadataFromClient: FileInfo, uploadedFile: File) =>
            logger.info(s"Mock server stored uploaded tmp file with name: ${uploadedFile.getName} (Metadata from client: $metadataFromClient)")
            complete(Future(uploadedFile.getName))
        }
      }
    }

    val bindingFuture = Http().newServerAt(address, port).bindFlow(routes)
    bindingFuture.onComplete {
      case Success(b) =>
        logger.info("Mock server started, listening on: {}", b.localAddress)
        serverBinding = bindingFuture
      case Failure(e) =>
        logger.info(s"Mock server could not bind to: $address:$port. Exception message: ${e.getMessage}")
        system.terminate()
    }

    sys.addShutdownHook {
      logger.info("About to shutdown...")
      val fut = bindingFuture.map(serverBinding => serverBinding.terminate(hardDeadline = 2.seconds))
      logger.info("Waiting for connections to terminate...")
      val onceAllConnectionsTerminated = Await.result(fut, 3.seconds)
      logger.info("Connections terminated")
      onceAllConnectionsTerminated.flatMap { _ => system.terminate()
      }
    }
  }

  def upload(file: File): Future[HttpResponse] = {
    val target = Uri(s"$protocol://$address:$port")
      .withPath(org.apache.pekko.http.scaladsl.model.Uri.Path("/api/upload"))

    val headers: Seq[HttpHeader] = Seq(RawHeader("accept", "*/*"))

    // Doc ConnectionPoolSettings
    // https://doc.akka.io/docs/akka-http/current/client-side/configuration.html#pool-settings
    val connectionPoolSettings = ConnectionPoolSettings(system)
      .withUpdatedConnectionSettings({ settings =>
        settings
          .withConnectingTimeout(10.seconds)
          .withIdleTimeout(2.minutes)
      })

    val result: Future[HttpResponse] =
      for {
        request <- createEntityFrom(file).map(entity => {
          val req = HttpRequest(HttpMethods.POST, uri = target, entity = entity).withHeaders(headers)
          logger.debug(s"Request URL: ${req._2}")
          logger.debug(s"Request headers: ${req._3}")
          logger.debug(s"Request entity: ${req._4}")
          logger.debug(s"Request attributes: ${req.attributes}")
          logger.debug(s"Request method: ${req.method}")
          req
        })
        response <- Http().singleRequest(request = request, settings = connectionPoolSettings)
      } yield response

    result.onComplete(res => logger.info(s"Upload client received result: $res"))
    result
  }

  def detectMediaType(file: File): ContentType = {
    val detectedMediaType = Files.probeContentType(file.toPath)
    logger.info(s"Detected media type: $detectedMediaType")

    ContentType.parse(detectedMediaType) match {
      case Right(contentType) => contentType
      case Left(_) => ContentTypes.`application/octet-stream`
    }
  }

  private def createEntityFrom(file: File): Future[RequestEntity] = {
    require(file.exists())
    val fileSource = FileIO.fromPath(file.toPath, chunkSize = 1000000)

    val formData = Multipart.FormData(Multipart.FormData.BodyPart(
      "uploadedFile",
      HttpEntity(detectMediaType(file), file.length(), fileSource),
      Map("filename" -> file.getName)))
    Marshal(formData).to[RequestEntity]
  }

  def stop(): Future[Terminated] = {
    logger.info("About to shutdown Uploader...")
    val fut = serverBinding.map(serverBinding => serverBinding.terminate(hardDeadline = 2.seconds))
    logger.info("Waiting for connections to terminate...")
    val onceAllConnectionsTerminated = Await.result(fut, 3.seconds)
    logger.info("Connections terminated")
    onceAllConnectionsTerminated.flatMap(_ => system.terminate())
  }

}

object Uploader {
  def main(args: Array[String]): Unit = {}
  def apply(system: ActorSystem) = new Uploader(system)
}

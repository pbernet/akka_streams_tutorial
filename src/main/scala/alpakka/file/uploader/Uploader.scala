package alpakka.file.uploader

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.marshalling.Marshal
import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.http.scaladsl.server.Directives.{complete, logRequestResult, path, _}
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.server.directives.FileInfo
import org.apache.pekko.http.scaladsl.settings.ConnectionPoolSettings
import org.apache.pekko.stream.scaladsl.FileIO
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

/**
  * Upload file, eg from file system
  * Is used by [[DirectoryListener]]
  *
  * Also starts a mock server to handle the uploaded files
  */
class Uploader(system: ActorSystem) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val systemImpl = system
  implicit val executionContext = system.dispatcher

  var serverBinding: Future[Http.ServerBinding] = _

  val (protocol, address, port) = ("http", "localhost", 6000)

  server(address, port)

  def server(address: String, port: Int): Unit = {

    def routes: Route = logRequestResult("uploader") {
      path("api" / "upload") {

        def tempDestination(fileInfo: FileInfo): File = File.createTempFile(fileInfo.fileName, ".tmp.server")

        storeUploadedFile("uploadedFile", tempDestination) {
          case (metadataFromClient: FileInfo, uploadedFile: File) =>
            logger.info(s"Server stored uploaded tmp file with name: ${uploadedFile.getName} (Metadata from client: $metadataFromClient)")
            complete(Future(uploadedFile.getName))
        }
      }
    }

    val bindingFuture = Http().newServerAt(address, port).bindFlow(routes)
    bindingFuture.onComplete {
      case Success(b) =>
        logger.info("Server started, listening on: " + b.localAddress)
        serverBinding = bindingFuture
      case Failure(e) =>
        logger.info(s"Server could not bind to $address:$port. Exception message: ${e.getMessage}")
        system.terminate()
    }

    sys.addShutdownHook {
      logger.info("About to shutdown...")
      val fut = bindingFuture.map(serverBinding => serverBinding.terminate(hardDeadline = 3.seconds))
      logger.info("Waiting for connections to terminate...")
      val onceAllConnectionsTerminated = Await.result(fut, 10.seconds)
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

  private def createEntityFrom(file: File): Future[RequestEntity] = {
    require(file.exists())
    val fileSource = FileIO.fromPath(file.toPath, chunkSize = 1000000)

    // akka-http server is easy regarding the MediaType
    // Other HTTP servers need an explicit MediaType, to be able to process the multipart POST request
    //    val paramMapFile = Map("type" -> "text/csv", "filename" -> file.getName)
    //
    //    val formData = Multipart.FormData(Multipart.FormData.BodyPart(
    //      "uploadedFile",
    //      //HttpEntity(MediaTypes.`multipart/form-data`, file.length(), fileSource), paramMapFile))
    //      HttpEntity(MediaTypes.`text/csv`.toContentType(HttpCharset.apply("UTF-8")(Seq.empty)), file.length(), fileSource), paramMapFile))

    val formData = Multipart.FormData(Multipart.FormData.BodyPart(
      "uploadedFile",
      HttpEntity(MediaTypes.`application/octet-stream`, file.length(), fileSource),
      Map("filename" -> file.getName)))
    Marshal(formData).to[RequestEntity]
  }

  def stop() = {
    logger.info("About to shutdown Uploader...")
    val fut = serverBinding.map(serverBinding => serverBinding.terminate(hardDeadline = 3.seconds))
    logger.info("Waiting for connections to terminate...")
    val onceAllConnectionsTerminated = Await.result(fut, 10.seconds)
    logger.info("Connections terminated")
    onceAllConnectionsTerminated.flatMap { _ => system.terminate()
    }
  }

}

object Uploader extends App {
  def apply(system: ActorSystem) = new Uploader(system)
}

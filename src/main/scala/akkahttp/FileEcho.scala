package akkahttp

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, MediaTypes, Multipart, _}
import akka.http.scaladsl.server.Directives.{complete, logRequestResult, path, _}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.FileInfo
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.FileIO
import spray.json.DefaultJsonProtocol

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}




trait MyJsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {
  final case class FileHandle(fileName: String, absolutePath: String, length: Long)
  implicit def fileInfoFormat = jsonFormat3(FileHandle.apply)
}


object FileEcho extends App with MyJsonProtocol {
  implicit val system = ActorSystem("FileEcho")
  implicit val executionContext = system.dispatcher
  implicit val materializerServer = ActorMaterializer()

  val (address, port) = ("127.0.0.1", 6000)
  server(address, port)
  uploadClient(address, port)


  def server(address: String, port: Int) = {

    def routes: Route = logRequestResult("fileecho") {
      path("upload") {

        def tempDestination(fileInfo: FileInfo): File = File.createTempFile(fileInfo.fileName, ".tmp")

        storeUploadedFile("binary", tempDestination) {
          case (metadataFromClient: FileInfo, uploadedFile: File) =>
            println(s"Saved uploaded tmp file with name: ${uploadedFile.getName} (Metadata from client: $metadataFromClient)")
            complete(Future(FileHandle(uploadedFile.getName, uploadedFile.getAbsolutePath, uploadedFile.length())))
        }
      }
    }

    val bindingFuture = Http().bindAndHandle(routes, address, port)
    bindingFuture.onComplete {
      case Success(b) =>
        println("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        println(s"Server could not bind to $address:$port. Exception message: ${e.getMessage}")
        system.terminate()
    }
  }

  def uploadClient(address: String, port: Int): Unit = {


    def createEntityFrom(file: File): Future[RequestEntity] = {
      require(file.exists())
      val fileSource = FileIO.fromPath(file.toPath, chunkSize = 100000)
      val formData = Multipart.FormData(Multipart.FormData.BodyPart(
        "binary",
        HttpEntity(MediaTypes.`application/octet-stream`, file.length(), fileSource),
        Map("filename" -> file.getName)))

      Marshal(formData).to[RequestEntity]
    }

    def upload(file: File): Unit = {
      try {
        val target = Uri(s"http://$address:$port").withPath(Path("/upload"))

        val result: Future[String] =
          for {
            request <- createEntityFrom(file).map(entity => HttpRequest(HttpMethods.POST, uri = target, entity = entity))
            response <- Http().singleRequest(request)
            responseBodyAsString <- Unmarshal(response).to[String]
          } yield responseBodyAsString

        result.onComplete { res ⇒
          println(s"Client received result: $res")
          system.terminate()
        }

        system.scheduler.scheduleOnce(60.seconds) {
          println("Shutting down after timeout...")
          system.terminate()
        }
      } catch {
        case _: Throwable => system.terminate()
      }
    }

    upload(new File(getClass.getResource("/testfile.txt").toURI))
  }

}

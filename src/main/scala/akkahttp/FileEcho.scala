package akkahttp

import java.io.File
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, MediaTypes, Multipart, RequestEntity, _}
import akka.http.scaladsl.server.Directives.{complete, logRequestResult, path, _}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.FileInfo
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Sink, Source}
import spray.json.DefaultJsonProtocol

import scala.concurrent.Future
import scala.util.{Failure, Success}


trait MyJsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {

  final case class FileHandle(fileName: String, absolutePath: String, length: Long)

  implicit def fileInfoFormat = jsonFormat3(FileHandle.apply)
}

/**
  * This file upload/download roundtrip example is inspired by:
  * https://github.com/clockfly/akka-http-file-server
  *
  */
object FileEcho extends App with MyJsonProtocol {
  implicit val system = ActorSystem("FileEcho")
  implicit val executionContext = system.dispatcher
  implicit val materializerServer = ActorMaterializer()

  val (address, port) = ("127.0.0.1", 6000)
  server(address, port)
  val fileHandle = uploadClient(address, port)
  fileHandle.flatMap(fileHandle => downloadClient(fileHandle, address, port))

  def server(address: String, port: Int): Unit = {

    def routes: Route = logRequestResult("fileecho") {
      path("upload") {

        def tempDestination(fileInfo: FileInfo): File = File.createTempFile(fileInfo.fileName, ".tmp.server")

        storeUploadedFile("binary", tempDestination) {
          case (metadataFromClient: FileInfo, uploadedFile: File) =>
            println(s"Server stored uploaded tmp file with name: ${uploadedFile.getName} (Metadata from client: $metadataFromClient)")
            complete(Future(FileHandle(uploadedFile.getName, uploadedFile.getAbsolutePath, uploadedFile.length())))
        }
      } ~
        path("download") {
          get {
            entity(as[FileHandle]) { fileHandle: FileHandle =>
              println(s"Server received download request for: ${fileHandle.fileName}")
              getFromFile(new File(fileHandle.absolutePath), MediaTypes.`application/octet-stream`)
            }
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

  def uploadClient(address: String, port: Int): Future[FileEcho.FileHandle] = {

    def createEntityFrom(file: File): Future[RequestEntity] = {
      require(file.exists())
      val fileSource = FileIO.fromPath(file.toPath, chunkSize = 100000)
      val formData = Multipart.FormData(Multipart.FormData.BodyPart(
        "binary",
        HttpEntity(MediaTypes.`application/octet-stream`, file.length(), fileSource),
        Map("filename" -> file.getName)))

      Marshal(formData).to[RequestEntity]
    }

    def upload(file: File): Future[FileHandle] = {

      val target = Uri(s"http://$address:$port").withPath(akka.http.scaladsl.model.Uri.Path("/upload"))

      val result: Future[FileHandle] =
        for {
          request <- createEntityFrom(file).map(entity => HttpRequest(HttpMethods.POST, uri = target, entity = entity))
          response <- Http().singleRequest(request)
          responseBodyAsString <- Unmarshal(response).to[FileHandle]
        } yield responseBodyAsString

      result.onComplete(res => println(s"Upload client received result: $res"))
      result
    }

    upload(new File(getClass.getResource("/testfile.txt").toURI))
  }

  def downloadClient(remoteFile: FileHandle, address: String, port: Int): Future[File] = {

    val target = Uri(s"http://$address:$port").withPath(akka.http.scaladsl.model.Uri.Path("/download"))
    val httpClient = Http(system).outgoingConnection(address, port)

    def download(remoteFileHandle: FileHandle, localFile: File): Future[Unit] = {

      val result = for {
        reqEntity <- Marshal(remoteFileHandle).to[RequestEntity]
        response <- Source.single(HttpRequest(HttpMethods.GET, uri = target, entity = reqEntity)).via(httpClient).runWith(Sink.head)
        downloaded <- response.entity.dataBytes.runWith(FileIO.toPath(Paths.get(localFile.getAbsolutePath)))
      } yield downloaded

      result.map{
        ioresult => println(s"Download client finished downloading: ${ioresult.count} bytes!")
        system.terminate()
      }
    }
    val localFile = File.createTempFile("downloadLocal", ".tmp.client")
    download(remoteFile, localFile)
    println(s"Download client will store file to: ${localFile.getAbsolutePath}")
    Future(localFile)
  }
}

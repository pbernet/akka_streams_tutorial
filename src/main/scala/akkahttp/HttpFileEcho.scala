package akkahttp

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{complete, logRequestResult, path, _}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.FileInfo
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.RestartSettings
import akka.stream.scaladsl.{Compression, FileIO, RestartSource, Sink, Source}
import spray.json.DefaultJsonProtocol

import java.io.File
import java.nio.file.Paths
import scala.collection.parallel.CollectionConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.sys.process.Process
import scala.util.{Failure, Success}

trait JsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {

  case class FileHandle(fileName: String, absolutePath: String, length: Long)

  implicit def fileInfoFormat = jsonFormat3(FileHandle)
}

/**
  * This HTTP file upload/download round trip is inspired by:
  * https://github.com/clockfly/akka-http-file-server
  *
  * Upload/download files up to the configured size in application.conf
  *
  * Added:
  *  - Retry on upload, Doc: https://blog.colinbreck.com/backoff-and-retry-error-handling-for-akka-streams
  *  - On the fly gzip compression on upload and gunzip decompression on download,
  *    Doc: https://doc.akka.io/docs/akka/current/stream/stream-cookbook.html#dealing-with-compressed-data-streams
  *  - Browser client for manual upload of uncompressed files
  *
  * To prove that the streaming works:
  *  - Replace testfile.jpg with a large file, eg 63MB.pdf
  *  - Run with limited Heap, eg with -Xms256m -Xmx256m
  *  - Monitor Heap, eg with visualvm.github.io
  *
  * TODO:
  *  - Retry on download
  *  - Investigate why chuckSizeBytes must be so large to handle large files
  */
object HttpFileEcho extends App with JsonProtocol {
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val resourceFileName = "testfile.jpg"
  val (address, port) = ("127.0.0.1", 6002)
  val chuckSizeBytes = 100 * 1024

  server(address, port)
  (1 to 10).par.foreach(each => roundtripClient(each, address, port))
  browserClient()

  def server(address: String, port: Int): Unit = {

    def routes: Route = logRequestResult("fileecho") {
      path("upload") {

        formFields(Symbol("payload")) { payload =>
          println(s"Server received request with additional form data: $payload")

          def tempDestination(fileInfo: FileInfo): File = File.createTempFile(fileInfo.fileName, ".tmp.server")

          storeUploadedFile("binary", tempDestination) {
            case (metadataFromClient: FileInfo, uploadedFile: File) =>
              println(s"Server stored uploaded tmp file with name: ${uploadedFile.getName} (Metadata from client: $metadataFromClient)")
              complete(Future(FileHandle(uploadedFile.getName, uploadedFile.getAbsolutePath, uploadedFile.length())))
          }
        }
      } ~
        path("download") {
          get {
            entity(as[FileHandle]) { fileHandle: FileHandle =>
              println(s"Server received download request for: ${fileHandle.fileName}")
              getFromFile(new File(fileHandle.absolutePath), MediaTypes.`application/octet-stream`)
            }
          }
        } ~
        get {
          val static = "src/main/resources"
          concat(
            pathSingleSlash {
              val appHtml = Paths.get(static, "fileupload.html").toFile
              getFromFile(appHtml, ContentTypes.`text/html(UTF-8)`)
            }
          )
        }
    }

    val bindingFuture = Http().newServerAt(address, port).bindFlow(routes)
    bindingFuture.onComplete {
      case Success(b) =>
        println("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        println(s"Server could not bind to $address:$port. Exception message: ${e.getMessage}")
        system.terminate()
    }

    sys.addShutdownHook {
      println("About to shutdown...")
      val fut = bindingFuture.map(serverBinding => serverBinding.terminate(hardDeadline = 3.seconds))
      println("Waiting for connections to terminate...")
      val onceAllConnectionsTerminated = Await.result(fut, 10.seconds)
      println("Connections terminated")
      onceAllConnectionsTerminated.flatMap { _ => system.terminate()
      }
    }
  }

  def roundtripClient(id: Int, address: String, port: Int) = {
    val fileHandle = uploadClient(id, address, port)
    fileHandle.onComplete {
      case Success(each) => downloadClient(id, each, address, port)
      case Failure(exception) => println(s"Exception during upload: $exception")
    }
  }

  def uploadClient(id: Int, address: String, port: Int): Future[HttpFileEcho.FileHandle] = {

    def createEntityFrom(file: File): Future[RequestEntity] = {
      require(file.exists())

      val compressedFileSource = FileIO.fromPath(file.toPath, chuckSizeBytes).via(Compression.gzip)
      val formData = Multipart.FormData(Multipart.FormData.BodyPart(
        "binary",
        HttpEntity(MediaTypes.`application/octet-stream`, file.length(), compressedFileSource),
        // Set the Content-Disposition header
        // see: https://www.w3.org/Protocols/HTTP/Issues/content-disposition.txt
        Map("filename" -> file.getName)),
        // Pass additional (json) payload in a form field
        Multipart.FormData.BodyPart.Strict("payload", "{\"payload\": \"sent from Scala client\"}", Map.empty)
      )

      Marshal(formData).to[RequestEntity]
    }

    def getResponse(request: HttpRequest): Future[FileHandle] = {
      val restartSettings = RestartSettings(1.second, 10.seconds, 0.2).withMaxRestarts(10, 1.minute)
      RestartSource.withBackoff(restartSettings) { () =>
        val responseFuture = Http().singleRequest(request)

        Source.future(responseFuture)
          .mapAsync(parallelism = 1) {
            case HttpResponse(StatusCodes.OK, _, entity, _) =>
              Unmarshal(entity).to[FileHandle]
            case HttpResponse(StatusCodes.InternalServerError, _, _, _) =>
              throw new RuntimeException(s"Response has status code: ${StatusCodes.InternalServerError}")
            case HttpResponse(statusCode, _, _, _) =>
              throw new RuntimeException(s"Response has status code: $statusCode")
          }
      }
        .runWith(Sink.head)
        .recover {
          case ex => throw new RuntimeException(s"Exception occurred: $ex")
        }
    }

    def upload(file: File): Future[FileHandle] = {

      def delayRequestSoTheServerIsNotHammered() = {
        val (start, end) = (1000, 5000)
        val rnd = new scala.util.Random
        val sleepTime = start + rnd.nextInt((end - start) + 1)
        Thread.sleep(sleepTime.toLong)
      }

      delayRequestSoTheServerIsNotHammered()

      val target = Uri(s"http://$address:$port").withPath(akka.http.scaladsl.model.Uri.Path("/upload"))

      val result: Future[FileHandle] =
        for {
          request <- createEntityFrom(file).map(entity => HttpRequest(HttpMethods.POST, uri = target, entity = entity))
          response <- getResponse(request)
          responseBodyAsString <- Unmarshal(response).to[FileHandle]
        } yield responseBodyAsString

      result.onComplete(res => println(s"UploadClient with id: $id received result: $res"))
      result
    }

    upload(Paths.get(s"src/main/resources/$resourceFileName").toFile)
  }

  def downloadClient(id: Int, remoteFile: FileHandle, address: String, port: Int): Future[File] = {

    val target = Uri(s"http://$address:$port").withPath(akka.http.scaladsl.model.Uri.Path("/download"))
    val httpClient = Http(system).outgoingConnection(address, port)

    def saveResponseToFile(response: HttpResponse, localFile: File) = {
      response.entity.dataBytes
        .via(Compression.gunzip(chuckSizeBytes))
        .runWith(FileIO.toPath(Paths.get(localFile.getAbsolutePath)))
    }

    def download(remoteFileHandle: FileHandle, localFile: File) = {

      val result = for {
        reqEntity <- Marshal(remoteFileHandle).to[RequestEntity]
        response <- Source.single(HttpRequest(HttpMethods.GET, uri = target, entity = reqEntity)).via(httpClient).runWith(Sink.head)
        downloaded <- saveResponseToFile(response, localFile)
      } yield downloaded

      val ioresult = Await.result(result, 10.seconds)
      println(s"DownloadClient with id: $id finished downloading: ${ioresult.count} bytes to file: ${localFile.getAbsolutePath}")
    }

    val localFile = File.createTempFile("downloadLocal", ".tmp.client")
    download(remoteFile, localFile)
    Future(localFile)
  }

  def browserClient() = {
    val os = System.getProperty("os.name").toLowerCase
    if (os == "mac os x") Process(s"open http://$address:$port").!
  }
}
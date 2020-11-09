package akkahttp

import java.io.File
import java.nio.file.Paths

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.Multipart.FormData
import akka.http.scaladsl.model.{HttpRequest, MediaTypes, RequestEntity, _}
import akka.http.scaladsl.server.Directives.{complete, logRequestResult, path, _}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.FileInfo
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.{FileIO, Keep, Sink, Source}
import akka.stream.{OverflowStrategy, QueueOfferResult}
import spray.json.DefaultJsonProtocol

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success}

/**
  * Differences to [[HttpFileEcho]]:
  *  - The upload client is processing a stream of FileHandle
  *  - The download client is using the host-level API with a SourceQueue
  *  - Number of retries set via config param max-retries in application.conf
  *
  * Doc:
  * https://doc.akka.io/docs/akka-http/current/client-side/host-level.html?language=scala#retrying-a-request
  *
  *
  * TODOs
  *  - When ex is thrown on server during download: The download retry does not seem to work as expected
  *  - When ex is thrown on server during download: The responseFuture is always a Success
  *  - Cleanup on pool shutdown
  *
  */
object HttpFileEchoStream extends App with DefaultJsonProtocol with SprayJsonSupport {
  implicit val system = ActorSystem("HttpFileEchoStream")
  implicit val executionContext = system.dispatcher

  final case class FileHandle(fileName: String, absolutePath: String, length: Long = 0)

  implicit def fileInfoFormat = jsonFormat3(FileHandle.apply)

  val resourceFileName = "testfile.jpg"
  val (address, port) = ("127.0.0.1", 6000)
  server(address, port)
  roundtripClient(address, port)

  def server(address: String, port: Int): Unit = {

    def routes: Route = logRequestResult("fileecho") {
      path("upload") {

        def tempDestination(fileInfo: FileInfo): File = File.createTempFile(fileInfo.fileName, ".tmp.server")

        storeUploadedFile("binary", tempDestination) {
          case (metadataFromClient: FileInfo, uploadedFile: File) =>
            println(s"Server: Stored uploaded tmp file with name: ${uploadedFile.getName} (Metadata from client: $metadataFromClient)")
            complete(Future(FileHandle(uploadedFile.getName, uploadedFile.getAbsolutePath, uploadedFile.length())))
        }
      } ~
        path("download") {
          get {
            entity(as[FileHandle]) { fileHandle: FileHandle =>
              //TODO see class comment
              //throw new RuntimeException("Boom server error")
              println(s"Server: Received download request for: ${fileHandle.fileName}")
              getFromFile(new File(fileHandle.absolutePath), MediaTypes.`application/octet-stream`)
            }
          }
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

  def filesToUpload(): Source[FileHandle, NotUsed] =
    //Unbounded stream. Limit for testing purposes by appending eg .take(5)
    Source(Stream.continually(FileHandle(resourceFileName, Paths.get(s"./src/main/resources/$resourceFileName").toString)))


  def roundtripClient(address: String, port: Int) = {

    val poolClientFlowUpload =
      Http().cachedHostConnectionPool[FileHandle](address, port)

    def createUploadRequest(fileToUpload: FileHandle): Future[(HttpRequest, FileHandle)] = {
      val bodyPart =
        FormData.BodyPart.fromPath("binary", ContentTypes.`application/octet-stream`, Paths.get(fileToUpload.absolutePath))

      val body = FormData(bodyPart) // only one file per upload
      Marshal(body).to[RequestEntity].map { entity => // use marshalling to create multipart/formdata entity
        val target = Uri(s"http://$address:$port").withPath(akka.http.scaladsl.model.Uri.Path("/upload"))
        HttpRequest(method = HttpMethods.POST, uri = target, entity = entity) -> fileToUpload
      }
    }


    def createDownloadRequest(fileToDownload: FileHandle): Future[HttpRequest] = {
      Marshal(fileToDownload).to[RequestEntity].map { entity: MessageEntity =>
        val target = Uri(s"http://$address:$port").withPath(akka.http.scaladsl.model.Uri.Path("/download"))
        HttpRequest(HttpMethods.GET, uri = target, entity = entity)
      }
    }

    def createDownloadRequestNoFuture(fileToDownload: FileHandle) ={
      val target = Uri(s"http://$address:$port").withPath(akka.http.scaladsl.model.Uri.Path("/download"))
      val entityFuture = Marshal(fileToDownload).to[MessageEntity]
      val entity = Await.result(entityFuture, 1.second) //TODO Do it without blocking
      HttpRequest(HttpMethods.GET, target, entity = entity)
    }


    def download(fileHandle: HttpFileEchoStream.FileHandle) = {
      val queueSize = 1
      val poolClientFlowDownload = Http().cachedHostConnectionPool[Promise[HttpResponse]](address, port)
      val queue =
        Source.queue[(HttpRequest, Promise[HttpResponse])](queueSize, OverflowStrategy.backpressure, 10)
          .via(poolClientFlowDownload)
          .toMat(Sink.foreach({
            case (Success(resp), p) => p.success(resp)
            case (Failure(e), p) => p.failure(e)
          }))(Keep.left)
          .run()

      def queueRequest(request: HttpRequest): Future[HttpResponse] = {
        val responsePromise = Promise[HttpResponse]()
        queue.offer(request -> responsePromise).flatMap {
          case QueueOfferResult.Enqueued => responsePromise.future
          case QueueOfferResult.Dropped => Future.failed(new RuntimeException("Queue overflowed. Try again later."))
          case QueueOfferResult.Failure(ex) => Future.failed(ex)
          case QueueOfferResult.QueueClosed => Future.failed(new RuntimeException("Queue was closed (pool shut down) while running the request. Try again later."))
        }
      }

      val responseFuture: Future[HttpResponse] = queueRequest(createDownloadRequestNoFuture(fileHandle))
      responseFuture.onComplete {
        case Success(resp) =>
          val localFile = File.createTempFile("downloadLocal", ".tmp.client")
          val result = resp.entity.dataBytes.runWith(FileIO.toPath(Paths.get(localFile.getAbsolutePath)))
          result.map {
            ioresult =>
              println(s"Client: Download file: $resp finished: ${ioresult.count} bytes!")
          }
        case Failure(exception) => println(s"Boom $exception while downloading")
      }
    }

    filesToUpload()
      // The stream will "pull out" these requests when capacity is available.
      // When that is the case we create one request concurrently
      // (the pipeline will still allow multiple requests running at the same time)
      .mapAsync(1)(createUploadRequest)
      // then dispatch the request to the connection pool
      .via(poolClientFlowUpload)
      // report each response
      // Note: responses will NOT come in in the same order as requests. The requests will be run on one of the
      // multiple pooled connections and may thus "overtake" each other!
      .runForeach {
      case (Success(response: HttpResponse), fileToUpload) =>
        println(s"Client: Upload for file: $fileToUpload was successful: ${response.status}")

        val fileHandleFuture = Unmarshal(response).to[FileHandle]
        val fileHandle = Await.result(fileHandleFuture, 1.second)  //TODO Do it without blocking
        response.discardEntityBytes()

        //Finish the roundtrip
        download(fileHandle)

      case (Failure(ex), fileToUpload) =>
        println(s"Uploading file $fileToUpload failed with $ex")
    }
  }
}
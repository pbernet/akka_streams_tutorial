package akkahttp

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.marshalling.Marshal
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.server.Directives.{complete, logRequestResult, path, *}
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.server.directives.FileInfo
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.stream.scaladsl.{FileIO, Keep, Sink, Source}
import org.apache.pekko.stream.{OverflowStrategy, QueueOfferResult, ThrottleMode}

import java.io.File
import java.nio.file.Paths
import java.time.LocalTime
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success}

/**
  * Differences to [[HttpFileEcho]]:
  *  - The upload client is processing a stream of FileHandle
  *  - The download client is using the host-level API with a SourceQueue
  *
  * Doc:
  * https://doc.akka.io/docs/akka-http/current/client-side/host-level.html#using-the-host-level-api-with-a-queue
  * https://doc.akka.io/docs/akka-http/current/client-side/host-level.html?language=scala#retrying-a-request
  *
  * Remarks:
  *  - No retry on upload because POST request is non-idempotent
  *  - Homegrown retry on download, because this does somehow not work yet via the cachedHostConnectionPool$
  *  - Shows more robust behaviour with large files than [[akkahttp.HttpFileEcho]]
  */
object HttpFileEchoStream extends App with JsonProtocol {
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val resourceFileName = "content/63MB.pdf"
  val (address, port) = ("127.0.0.1", 6000)
  server(address, port)
  roundtripClient(address, port)

  def server(address: String, port: Int): Unit = {

    def throwRndRuntimeException(operation: String): Unit = {
      val time = LocalTime.now()
      if (time.getSecond % 2 == 0) {
        val msg = s"Server RuntimeException during $operation at: $time"
        println(msg)
        throw new RuntimeException(s"BOOM - $msg")
      }
    }

    def routes: Route = logRequestResult("fileecho") {
      path("upload") {

        def tempDestination(fileInfo: FileInfo): File = File.createTempFile(fileInfo.fileName, ".tmp.server")

        storeUploadedFile("binary", tempDestination) {
          case (metadataFromClient: FileInfo, uploadedFile: File) =>
            println(s"Server: Stored uploaded tmp file with name: ${uploadedFile.getName} (Metadata from client: $metadataFromClient)")

            // Activate to simulate rnd server ex during upload
            //throwRndRuntimeException("upload")

            complete(Future(FileHandle(uploadedFile.getName, uploadedFile.getAbsolutePath, uploadedFile.length())))
        }
      } ~
        path("download") {
          get {
            entity(as[FileHandle]) { fileHandle =>
              println(s"Server: Received download request for: ${fileHandle.fileName}")

              // Activate to simulate rnd server ex during download
              //throwRndRuntimeException("download")

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


  def roundtripClient(address: String, port: Int) = {

    val filesToUpload =
      // Unbounded stream. Limited for testing purposes by appending eg .take(n)
      Source(LazyList.continually(FileHandle(resourceFileName, Paths.get(s"src/main/resources/$resourceFileName").toString, 0))).take(100)

    val hostConnectionPoolUpload = Http().cachedHostConnectionPool[FileHandle](address, port)

    def createEntityFrom(file: File): Future[RequestEntity] = {
      require(file.exists())
      val fileSource = FileIO.fromPath(file.toPath, chunkSize = 1000000)
      val formData = Multipart.FormData(Multipart.FormData.BodyPart(
        "binary",
        HttpEntity(MediaTypes.`application/octet-stream`, file.length(), fileSource),
        Map("filename" -> file.getName)))

      Marshal(formData).to[RequestEntity]
    }

    def createUploadRequest(fileToUpload: FileHandle): Future[(HttpRequest, FileHandle)] = {
      val target = Uri(s"http://$address:$port").withPath(org.apache.pekko.http.scaladsl.model.Uri.Path("/upload"))

      createEntityFrom(new File(fileToUpload.absolutePath))
        .map(entity => HttpRequest(HttpMethods.POST, uri = target, entity = entity))
        .map(each => (each, fileToUpload))
    }


    def createDownloadRequest(fileToDownload: FileHandle): Future[HttpRequest] = {
      Marshal(fileToDownload).to[RequestEntity].map { entity =>
        val target = Uri(s"http://$address:$port").withPath(org.apache.pekko.http.scaladsl.model.Uri.Path("/download"))
        HttpRequest(HttpMethods.GET, uri = target, entity = entity)
      }
    }

    def createDownloadRequestBlocking(fileToDownload: FileHandle) = {
      val target = Uri(s"http://$address:$port").withPath(org.apache.pekko.http.scaladsl.model.Uri.Path("/download"))
      val entityFuture = Marshal(fileToDownload).to[MessageEntity]
      val entity = Await.result(entityFuture, 1.second)
      HttpRequest(HttpMethods.GET, target, entity = entity)
    }


    def download(fileHandle: FileHandle) = {
      val queueSize = 1
      val hostConnectionPoolDownload = Http().cachedHostConnectionPool[Promise[HttpResponse]](address, port)
      val queue =
        Source.queue[(HttpRequest, Promise[HttpResponse])](queueSize, OverflowStrategy.backpressure, 10)
          .via(hostConnectionPoolDownload)
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

      def downloadRetry(fileHandle: FileHandle): Future[NotUsed] = {
        queueRequest(createDownloadRequestBlocking(fileHandle)).flatMap(response =>

          if (response.status.isSuccess()) {
            val localFile = File.createTempFile("downloadLocal", ".tmp.client")
            val result = response.entity.dataBytes.runWith(FileIO.toPath(Paths.get(localFile.getAbsolutePath)))
            result.map {
              ioresult =>
                println(s"Client: Finished download file: $response (size: ${ioresult.count} bytes)")
            }
          } else {
            throw new RuntimeException("Retry")
          }
        ).recoverWith {
          case ex: RuntimeException =>
            println(s"About to retry download, because of: $ex")
            downloadRetry(fileHandle)
          case e: Throwable => Future.failed(e)
        }
        Future(NotUsed)
      }

      downloadRetry(fileHandle)
    }

    filesToUpload
      .throttle(1, 1.second, 10, ThrottleMode.shaping)
      // The stream will "pull out" these requests when capacity is available.
      // When that is the case we create one request concurrently
      // (the pipeline will still allow multiple requests running at the same time)
      .mapAsync(1)(createUploadRequest)
      // then dispatch the request to the connection pool
      .via(hostConnectionPoolUpload)
      // report each response
      // Note: responses will NOT come in in the same order as requests. The requests will be run on one of the
      // multiple pooled connections and may thus "overtake" each other!
      .runForeach {
        case (Success(response: HttpResponse), fileToUpload) =>
          println(s"Client: Uploaded file: $fileToUpload (status: ${response.status})")

          val fileHandleFuture = Unmarshal(response.entity).to[FileHandle]
          val fileHandle = Await.result(fileHandleFuture, 1.second)

          // Finish the roundtrip
          download(fileHandle)

        case (Failure(ex), fileToUpload) =>
          println(s"Uploading file: $fileToUpload failed with: $ex")
      }
  }
}

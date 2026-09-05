package akkahttp

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.marshalling.Marshal
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.server.directives.FileInfo
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.pattern.after
import org.apache.pekko.stream.ThrottleMode
import org.apache.pekko.stream.scaladsl.{FileIO, Sink, Source}
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.nio.file.Paths
import java.time.LocalTime
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

/**
  * Differences to [[HttpFileEcho]]:
  *  - The upload client is processing a stream of [[FileHandle]]
  *  - The download client is using the host-level API with a one-element source
  *
  * Doc:
  * https://pekko.apache.org/docs/pekko-http/current/client-side/host-level.html
  * https://pekko.apache.org/docs/pekko-http/current/client-side/host-level.html#retrying-a-request
  *
  * Remarks:
  *  - No retry on upload because POST request is non-idempotent
  *  - The cached host connection pool retries transport failures for the idempotent download GET request.
  *    A bounded application-level retry handles HTTP 5xx responses, which are valid responses and therefore
  *    are intentionally not retried by the host connection pool
  *  - Shows more robust behavior with large files than [[akkahttp.HttpFileEcho]]
  */
object HttpFileEchoStream extends JsonProtocol {
  def main(args: Array[String]): Unit = {
    server(address, port)
    roundtripClient(address, port)
  }
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit lazy val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val resourceFileName = "content/63MB.pdf"
  val (address, port) = ("127.0.0.1", 6000)
  def server(address: String, port: Int): Unit = {

    def throwRndRuntimeException(operation: String): Unit = {
      val time = LocalTime.now()
      if (time.getSecond % 2 == 0) {
        val msg = s"Server RuntimeException during: $operation at: $time"
        logger.error(msg)
        throw new RuntimeException(s"BOOM - $msg")
      }
    }

    def routes: Route = logRequestResult("fileecho") {
      path("upload") {

        def tempDestination(fileInfo: FileInfo): File = File.createTempFile(fileInfo.fileName, ".tmp.server")

        storeUploadedFile("binary", tempDestination) {
          case (metadataFromClient: FileInfo, uploadedFile: File) =>
            logger.info(s"Server: Stored uploaded tmp file with name: ${uploadedFile.getName} (Metadata from client: $metadataFromClient)")

            // Activate to simulate rnd server ex during upload
            //throwRndRuntimeException("upload")

            complete(Future(FileHandle(uploadedFile.getName, uploadedFile.getAbsolutePath, uploadedFile.length())))
        }
      } ~
        path("download") {
          get {
            entity(as[FileHandle]) { fileHandle =>
              logger.info(s"Server: Received download request for: ${fileHandle.fileName}")

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
        logger.info(s"Server started, listening on: ${b.localAddress}")
      case Failure(e) =>
        logger.error(s"Server could not bind to $address:$port", e)
        system.terminate()
    }
    sys.addShutdownHook {
      logger.info("About to shut down...")
      val fut = bindingFuture.map(serverBinding => serverBinding.terminate(hardDeadline = 3.seconds))
      logger.info("Waiting for connections to terminate...")
      val onceAllConnectionsTerminated = Await.result(fut, 10.seconds)
      logger.info("Connections terminated")
      onceAllConnectionsTerminated.flatMap { _ => system.terminate()
      }
    }
  }


  def roundtripClient(address: String, port: Int) = {

    val filesToUpload =
      // Unbounded stream. Limit for testing purposes by appending .take(n)
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
      val target = Uri(s"http://$address:$port").withPath(Uri.Path("/upload"))

      createEntityFrom(new File(fileToUpload.absolutePath))
        .map(entity => HttpRequest(HttpMethods.POST, uri = target, entity = entity))
        .map(each => (each, fileToUpload))
    }


    def createDownloadRequest(fileToDownload: FileHandle): Future[HttpRequest] = {
      Marshal(fileToDownload).to[RequestEntity].map { entity =>
        val target = Uri(s"http://$address:$port").withPath(Uri.Path("/download"))
        HttpRequest(HttpMethods.GET, uri = target, entity = entity)
      }
    }

    def createDownloadRequestBlocking(fileToDownload: FileHandle) = {
      val target = Uri(s"http://$address:$port").withPath(Uri.Path("/download"))
      val entityFuture = Marshal(fileToDownload).to[MessageEntity]
      val entity = Await.result(entityFuture, 1.second)
      HttpRequest(HttpMethods.GET, target, entity = entity)
    }


    def download(fileHandle: FileHandle): Future[Unit] = {
      val hostConnectionPoolDownload = Http().cachedHostConnectionPool[Unit](address, port)

      def executeRequest(request: HttpRequest): Future[HttpResponse] = {
        Source.single(request -> ())
          .via(hostConnectionPoolDownload)
          .runWith(Sink.head)
          .flatMap {
            case (Success(response), _) => Future.successful(response)
            case (Failure(exception), _) => Future.failed(exception)
          }
      }

      def downloadWithRetry(fileHandle: FileHandle, retriesRemaining: Int): Future[Unit] = {
        executeRequest(createDownloadRequestBlocking(fileHandle)).flatMap { response =>
          if (response.status.isSuccess()) {
            val localFile = File.createTempFile("downloadLocal", ".tmp.client")
            response.entity.dataBytes
              .runWith(FileIO.toPath(localFile.toPath))
              .map { ioResult =>
                logger.info(s"Client: Finished download file: $response (size: ${ioResult.count} bytes)")
              }
          } else {
            val status = response.status
            response.discardEntityBytes().future.flatMap { _ =>
              if (status.intValue() >= 500 && status.intValue() < 600 && retriesRemaining > 0) {
                logger.warn(s"Download returned: $status. Retries remaining: $retriesRemaining")
                after(1.second, system.scheduler) {
                  downloadWithRetry(fileHandle, retriesRemaining - 1)
                }
              } else {
                Future.failed(new RuntimeException(s"Download failed with status: $status"))
              }
            }
          }
        }
      }

      downloadWithRetry(fileHandle, retriesRemaining = 5)
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
      // Note: responses will NOT come in the same order as requests. The requests will be run on one of the
      // multiple pooled connections and may thus "overtake" each other!
      .mapAsync(1) {
        case (Success(response: HttpResponse), fileToUpload) =>
          logger.info(s"Client: Uploaded file: $fileToUpload (status: ${response.status})")

          // Keep the download Future in the stream so completion and failures are observed.
          Unmarshal(response.entity)
            .to[FileHandle]
            .flatMap(download)

        case (Failure(ex), fileToUpload) =>
          logger.error(s"Uploading file failed: $fileToUpload", ex)
          Future.unit
      }
      .runWith(Sink.ignore)
  }
}

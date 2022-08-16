package akka.grpc.fileupload

import akka.NotUsed
import akka.grpc.fileupload.gen.{FileService, FileUploadRequest, FileUploadResponse}
import akka.stream.Materializer
import akka.stream.scaladsl.{FileIO, Sink, Source}
import akka.util.ByteString
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.Paths
import scala.concurrent.Future

/**
  * Basic file upload example, using akka-streams based on the suggested
  * protocol of this Java grpc example:
  * https://www.vinsguru.com/grpc-file-upload-client-streaming
  *
  * Remarks:
  *  - Protocol buffers are just providing the encoding of the data,
  *    the network protocol (for the chunked file upload) we have to implement ourselves
  *
  *  - Since it is difficult to run to a Sink with a dynamic file name,
  *    we use the alsoTo operator as a workaround to achieve this, see:
  *    https://stackoverflow.com/questions/45192072/using-dynamic-sink-destination-in-akka-streams
  *    https://github.com/akka/akka/issues/18969
  *
  *  - Using akka-streams we get a Source with file chunks,
  *    instead of a hook to [[io.grpc.stub.StreamObserver]] as in the Java example.
  *    Dealing with server failures thus may become more cumbersome.
  *
  *
  * For details about prefixAndTail/flatMapConcat see: [[HandleFirstElementSpecially]]
  *
  */
class FileServiceImpl(implicit mat: Materializer) extends FileService {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  import mat.executionContext

  override def upload(in: Source[FileUploadRequest, NotUsed]): Future[FileUploadResponse] = {
    in.prefixAndTail(1).flatMapConcat { case (head: Seq[FileUploadRequest], tail: Source[FileUploadRequest, NotUsed]) =>
      val fileName = s"${head.head.getMetadata.name}.${head.head.getMetadata.`type`}"
      val tmpPath = Paths.get(System.getProperty("java.io.tmpdir")).resolve(fileName)
      val dynamicSink = FileIO.toPath(tmpPath)

      logger.info(s"About to stream uploaded file to: $tmpPath...")
      tail
        .map(chunk => ByteString(chunk.getFile.content.toByteArray))
        .alsoTo(dynamicSink)
    }.runWith(Sink.ignore).map(done => FileUploadResponse.defaultInstance.withStatus(akka.grpc.fileupload.gen.Status.SUCCESS))
  }
}
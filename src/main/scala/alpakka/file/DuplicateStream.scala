package alpakka.file

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{FileIO, Sink, Source, StreamConverters}
import org.apache.pekko.util.ByteString

import java.io.FileInputStream
import java.nio.file.Paths
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Process FileInputStream in two steps:
  *  - Duplicate with alsoTo
  *  - Do sth else with original stream
  *    Should also work with ByteArrayInputStream
  */
object DuplicateStream extends App {
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val sourceFileName = "63MB.pdf"
  val sourceFilePath = s"src/main/resources/$sourceFileName"
  val fileInputStream = new FileInputStream(sourceFilePath)
  val source: Source[ByteString, Any] =
    StreamConverters.fromInputStream(() => fileInputStream, chunkSize = 10 * 1024)

  val alsoToSink = FileIO.toPath(Paths.get("outputAlsoTo.pdf"))
  val alsoToSink2 = FileIO.toPath(Paths.get("outputAlsoTo2.pdf"))

  val sink = FileIO.toPath(Paths.get("output.pdf"))

  // Step 1
  val done: Future[Seq[ByteString]] = source
    .alsoTo(alsoToSink)
    .alsoTo(alsoToSink2)
    .runWith(Sink.seq)

  // Step 2
  done.onComplete {
    case Success(seq) =>
      println("Continue processing...")
      Source(seq).runWith(sink)
      system.terminate()
    case Failure(e) =>
      println(s"Failure: $e")
      system.terminate()
  }
}

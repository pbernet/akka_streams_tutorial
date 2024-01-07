package alpakka.xml

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.connectors.xml.scaladsl.XmlParsing
import org.apache.pekko.stream.connectors.xml.{EndElement, ParseEvent, StartElement, TextEvent}
import org.apache.pekko.stream.scaladsl.{FileIO, Sink, Source}
import org.apache.pekko.util.ByteString

import java.nio.file.Paths
import java.util.Base64
import scala.collection.immutable
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Parse XML file to get a stream of consecutive events of type `ParseEvent`.
  * As a side effect:
  * Detect embedded base64 encoded `application/jpg`, extract and decode it
  * in memory and write to file
  *
  */

object XmlProcessing extends App {
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val resultFileName = "extracted_from_xml"

  val done = FileIO.fromPath(Paths.get("src/main/resources/xml_with_base64_embedded.xml"))
    .via(XmlParsing.parser)
    .statefulMapConcat(() => {

      // state
      val base64ContentAggregator = new StringBuilder()
      var counter = 0
      var fileEnding = ""

      // aggregation function
      parseEvent: ParseEvent =>
        parseEvent match {
          case s: StartElement if s.attributes.contains("mediaType") =>
            base64ContentAggregator.clear()
            val mediaType = s.attributes.head._2
            fileEnding = mediaType.split("/").toList.reverse.head
            println(s"mediaType: $mediaType / file ending: $fileEnding")
            immutable.Seq.empty
          case s: EndElement if s.localName == "embeddedDoc" =>
            val base64Content = base64ContentAggregator.toString
            Source.single(ByteString(base64Content))
              .map(each => ByteString(Base64.getMimeDecoder.decode(each.toByteBuffer)))
              .runWith(FileIO.toPath(Paths.get(s"$counter-$resultFileName.$fileEnding")))
            counter = counter + 1
            immutable.Seq.empty
          case t: TextEvent =>
            println(s"TextEvent with chunked content: ${t.text}")
            base64ContentAggregator.append(t.text)
            immutable.Seq.empty
          case _ =>
            immutable.Seq.empty
        }
    })
    .runWith(Sink.ignore)

  terminateWhen(done)


  def terminateWhen(done: Future[_]) = {
    done.onComplete {
      case Success(_) =>
        println("Flow Success. About to terminate...")
        system.terminate()
      case Failure(e) =>
        println(s"Flow Failure: $e. About to terminate...")
        system.terminate()
    }
  }
}

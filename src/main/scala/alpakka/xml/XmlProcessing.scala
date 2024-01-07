package alpakka.xml

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.connectors.xml.scaladsl.XmlParsing
import org.apache.pekko.stream.connectors.xml.{EndElement, StartElement, TextEvent}
import org.apache.pekko.stream.scaladsl.{FileIO, Sink, Source}
import org.apache.pekko.util.ByteString

import java.nio.file.Paths
import java.util.Base64
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Parse XML file to get a stream of consecutive events of type `ParseEvent`.
  *
  * As a side effect:
  * Detect embedded base64 encoded `application/jpg` files,
  * extract and decode them in memory and write to disk
  *
  */

object XmlProcessing extends App {
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val resultFileName = "extracted_from_xml"

  val done = FileIO.fromPath(Paths.get("src/main/resources/xml_with_base64_embedded.xml"))
    .via(XmlParsing.parser)
    .statefulMap(() => State(new StringBuilder(), 1, ""))(
      (state, nextElem) => {
        nextElem match {
          case s: StartElement if s.attributes.contains("mediaType") =>
            state.base64Content.clear()
            val mediaType = s.attributes.head._2
            val fileEnding = mediaType.split("/").toList.reverse.head
            println(s"mediaType: $mediaType / file ending: $fileEnding")
            state.fileEnding = fileEnding
            (state, Nil)
          case s: EndElement if s.localName == "embeddedDoc" =>
            Source.single(ByteString(state.base64Content.toString))
              .map(each => ByteString(Base64.getMimeDecoder.decode(each.toByteBuffer)))
              .runWith(FileIO.toPath(Paths.get(s"${state.counter}-$resultFileName.${state.fileEnding}")))
            state.counter = state.counter + 1
            (state, Nil)
          case t: TextEvent =>
            println(s"TextEvent with (chunked) content: ${t.text}")
            state.base64Content.append(t.text)
            (state, Nil)
          case _ =>
            (state, Nil)
        }
      },
      // Cleanup function, we return the last state
      state => Some(state))

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

  case class State(base64Content: StringBuilder, var counter: Int, var fileEnding: String)
}

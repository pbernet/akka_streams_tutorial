package alpakka.xml

import java.nio.file.Paths
import java.util.Base64

import akka.actor.ActorSystem
import akka.stream.alpakka.xml.scaladsl.XmlParsing
import akka.stream.alpakka.xml.{EndElement, ParseEvent, StartElement, TextEvent}
import akka.stream.scaladsl.{FileIO, Sink, Source}
import akka.util.ByteString

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Parse XML and extract embedded base64 application/jpg
  * and stream results to file
  *
  * Remarks:
  * - A case class could be used inside statefulMapConcat, to model the state
  * - The payload could be streamed directly to the result file to avoid memory overhead
  *
  */

object XmlProcessing extends App {
  implicit val system = ActorSystem("XmlProcessing")
  implicit val executionContext = system.dispatcher

  val resultFileName = "testfile_result.jpg"

  val done = FileIO.fromPath(Paths.get("./src/main/resources/xml_with_base64_embedded.xml"))
    .via(XmlParsing.parser)
    .statefulMapConcat(() => {

      // state
      val stringBuilder: StringBuilder = new StringBuilder()
      var counter: Int = 0

      // aggregation function
      parseEvent: ParseEvent =>
        parseEvent match {
          case s: StartElement if s.attributes.contains("mediaType") =>
            stringBuilder.clear()
            val mediaType = s.attributes.head._2
            println("mediaType: " + mediaType)
            immutable.Seq(mediaType)
          case s: EndElement if s.localName == "embeddedDoc" =>
            val text = stringBuilder.toString
            println("File content: " + text) //large embedded files are read into memory
            Source.single(ByteString(text))
              .map(each => ByteString(Base64.getMimeDecoder.decode(each.toByteBuffer)))
              .runWith(FileIO.toPath(Paths.get(s"$counter-$resultFileName")))
            counter = counter + 1
            immutable.Seq(text)
          case t: TextEvent =>
            stringBuilder.append(t.text)
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

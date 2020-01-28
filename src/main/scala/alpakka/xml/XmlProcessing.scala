package alpakka.xml

import java.io.{BufferedOutputStream, FileOutputStream}
import java.nio.file.Paths
import java.util.Base64

import akka.actor.ActorSystem
import akka.stream.alpakka.xml.scaladsl.XmlParsing
import akka.stream.alpakka.xml.{EndElement, ParseEvent, StartElement, TextEvent}
import akka.stream.scaladsl.{FileIO, Sink}

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.Try

/**
  * Parse XML and extract embedded base64 application/pdf docs
  *
  * Remarks:
  * - Extraction of the file name is missing,
  *   all the files are saved under the same dummy name
  * - State case class could be used inside statefulMapConcat
  * - Results could be written to fileSink (FileIO.toPath)
  */

object XmlProcessing {
  implicit val system = ActorSystem("XmlProcessing")
  implicit val executionContext = system.dispatcher

  def main(args: Array[String]): Unit = {
    val result: Future[immutable.Seq[String]] = FileIO.fromPath(Paths.get("./src/main/resources/largeEmbeddedPDF.xml"))
      .via(XmlParsing.parser)
      .statefulMapConcat(() => {
        // state
        val textBuffer = StringBuilder.newBuilder
        // aggregation function
        parseEvent: ParseEvent =>
          parseEvent match {
            case s: StartElement if s.attributes.contains("mediaType") =>
              textBuffer.clear()
              val mediaType = s.attributes.head._2
              println("Add mediaType: " + mediaType)
              immutable.Seq(mediaType)
            case s: EndElement if s.localName == "observationMedia" =>
              val text = textBuffer.toString
              println("Add payload: " + text)  //Note that large embedded PDFs are read into memory
              immutable.Seq(text)
            case t: TextEvent =>
              textBuffer.append(t.text)
              immutable.Seq.empty
            case _ =>
              immutable.Seq.empty
          }
      })
      .runWith(Sink.seq)

    result.onComplete {
      results: Try[immutable.Seq[String]] =>
        results.get.sliding(2, 2).toList.filter(each => each.head == "application/pdf").foreach {
          each: immutable.Seq[String] => {
            val dec1 = Base64.getMimeDecoder
            val decoded: Array[Byte] = dec1.decode(each.reverse.head)
            val bos = new BufferedOutputStream(new FileOutputStream("test.pdf"))
            bos.write(decoded)
            bos.close()
          }
        }

        println("Flow completed, about to terminate")
        system.terminate()
    }
  }
}

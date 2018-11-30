package alpakka.xml

import java.io.{BufferedOutputStream, FileOutputStream}
import java.util.Base64

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.xml.scaladsl.XmlParsing
import akka.stream.alpakka.xml.{EndElement, ParseEvent, StartElement, TextEvent}
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString

import scala.collection.immutable
import scala.concurrent.Future

/**
  * Parse XML and extract embedded base64 docs
  *
  */

object XmlProcessing {
  implicit val system = ActorSystem("XmlProcessing")
  implicit val executionContext = system.dispatcher
  implicit val materializer = ActorMaterializer()

  def main(args: Array[String]): Unit = {
    val fileContents = scala.io.Source.fromFile("./src/main/resources/CDA_Level_3.xml").getLines.mkString
    val byteStringDoc = ByteString.fromString(fileContents, "UTF-8")

    val result: Future[immutable.Seq[String]] = Source
      .single(byteStringDoc)
      .via(XmlParsing.parser)
      .statefulMapConcat(() => {
        // state
        val textBuffer = StringBuilder.newBuilder
        // aggregation function
        parseEvent: ParseEvent =>
          parseEvent match {
            case s: StartElement if s.attributes.contains("mediaType") =>
              println("Found application/pdf: " + s.attributes.head)
              textBuffer.clear()
              immutable.Seq.empty
            case s: EndElement if s.localName == "observationMedia" =>
              val text = textBuffer.toString
              println("Found text: " + text)
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
      results =>
        results.get.foreach {
          each: String => {
            val dec1 = Base64.getMimeDecoder
            println("Each base64: " + each)
            println("----")
            val decoded: Array[Byte] = dec1.decode(each)
            val bos = new BufferedOutputStream(new FileOutputStream("test.jpeg"))
            bos.write(decoded)
            bos.close()
          }
        }

        println("Flow completed with results: " + results + " - about to terminate")
        system.terminate()
    }
  }
}

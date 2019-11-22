package sample.stream

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}

/**
  * Inspired by:
  * https://stackoverflow.com/questions/35120082/how-to-get-started-with-akka-streams
  *
  */
object BasicTransformation {
  implicit val system = ActorSystem("BasicTransformation")
  import system.dispatcher

  def main(args: Array[String]): Unit = {
    val text =
      """|Lorem Ipsum is simply dummy text of the printing and typesetting industry.
         |Lorem Ipsum has been the industry's standard dummy text ever since the 1500s,
         |when an unknown printer took a galley of type and scrambled it to make a type
         |specimen book.""".stripMargin

    val source = Source.fromIterator(() => text.split("\\s").iterator)
    val sink = Sink.foreach[String](println)
    val flow = Flow[String].map(x => x.toUpperCase)
    val result = source.via(flow).runWith(sink)
    result.onComplete(_ => system.terminate())
  }
}

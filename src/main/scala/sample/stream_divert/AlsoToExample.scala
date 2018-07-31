package sample.stream_divert

import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, Attributes}

/**
  * Example with alsoTo and akka streams logging
  *
  * Inspired by:
  * https://blog.softwaremill.com/akka-streams-pitfalls-to-avoid-part-2-f93e60746c58
  * https://doc.akka.io/docs/akka/2.5/stream/stream-cookbook.html?language=scala#logging-in-streams
  *
  * Issues:
  * * how can the customLogger be configured in application.conf?
  * * how does logging work in the alsoTo path?
  *
  */

object AlsoToExample {
  implicit val system = ActorSystem("AlsoToExample")
  implicit val executionContext = system.dispatcher
  implicit val materializerServer = ActorMaterializer()
  //implicit val adapter = Logging(system, "customLogger")

  def main(args: Array[String]) {
    val source = Source(1 to 10)
    val sink = Sink.foreach{x: Int => println(s"elem: $x to sink")}

    val flow = Flow[Int]
      .log("before alsoTo")
      .withAttributes(
      Attributes.logLevels(
        onElement = Logging.InfoLevel,
        onFinish = Logging.InfoLevel,
        onFailure = Logging.DebugLevel
      ))
      .alsoTo(Sink.foreach { x: Int =>
        Thread.sleep(100)
        println(s" elem: $x alsoTo sink")
      }
      )
      .log("after alsoTo")
      .withAttributes(
        Attributes.logLevels(
          onElement = Logging.InfoLevel,
          onFinish = Logging.InfoLevel,
          onFailure = Logging.DebugLevel
        ))

    val result = source.via(flow).runWith(sink)
    result.onComplete(_ => system.terminate())
  }
}

package sample.stream_divert

import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.Attributes
import akka.stream.scaladsl.{Flow, Sink, Source}

/**
  * Shows the async nature of the alsoTo operator.
  * Uses the provided akka streams [[akka.event.LoggingAdapter]] (backed by slf4j/logback)
  *
  * Inspired by:
  * https://blog.softwaremill.com/akka-streams-pitfalls-to-avoid-part-2-f93e60746c58
  * https://doc.akka.io/docs/akka/current/stream/stream-cookbook.html?language=scala#logging-in-streams
  *
  */

object AlsoTo extends App {
  implicit val system: ActorSystem = ActorSystem()
  implicit val adapter = Logging(system, this.getClass)

  import system.dispatcher

  val source = Source(1 to 10)

  val sink = Sink.foreach { x: Int => adapter.log(Logging.InfoLevel, s" --> Element: $x reached sink") }

  def sinkBlocking = Sink.foreach { x: Int =>
    Thread.sleep(1000)
    adapter.log(Logging.InfoLevel, s" --> Element: $x logged in alsoTo sinkBlocking by ${Thread.currentThread().getName}")
  }

  val flow = Flow[Int]
    .log("before alsoTo")
    .alsoTo(sinkBlocking)
    .log("after alsoTo")
    .withAttributes(
      Attributes.logLevels(
        onElement = Logging.InfoLevel,
        onFinish = Logging.InfoLevel,
        onFailure = Logging.DebugLevel
      ))

  val done = source.via(flow).runWith(sink)
  done.onComplete(_ => system.terminate())
}

package sample.stream_divert

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.event.{Logging, LoggingAdapter}
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}

/**
  * Shows the async nature of the alsoTo operator.
  * Uses the provided akka streams [[akka.event.LoggingAdapter]] (backed by slf4j/logback)
  *
  * Inspired by:
  * https://blog.softwaremill.com/akka-streams-pitfalls-to-avoid-part-2-f93e60746c58
  * https://doc.akka.io/docs/akka/current/stream/stream-cookbook.html?language=scala#logging-in-streams
  *
  */

object AlsoTo {
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem()
    implicit val adapter: LoggingAdapter = Logging(system, "AlsoTo")

    import system.dispatcher

    val source = Source(1 to 10)

    val sink = Sink.foreach((value: Int) => adapter.log(Logging.InfoLevel, s" --> Element: $value reached sink"))

    def sinkBlocking = Sink.foreach { (value: Int) =>
      Thread.sleep(1000)
      adapter.log(Logging.InfoLevel, s" --> Element: $value logged in alsoTo sinkBlocking by ${Thread.currentThread().getName}")
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
}

package sample.stream

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success}

/**
  * Inspired by:
  * https://stackoverflow.com/questions/70921445/how-to-handle-java-util-nosuchelementexception-reduce-over-empty-stream-akka-st
  *
  * Gracefully fold/reduce over a possibly empty stream (eg due to filtering)
  *
  */
object PossiblyEmpty extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val printSink = Sink.foreach[Int](each => logger.info(s"Reached sink: $each"))

  val possiblyEmpty = Source(Seq(1, 3, 5)) // vs Seq(1, 2, 3, 5)
    .filter(_ % 2 == 0)
    .fold(0)(_ + _) // fold allows for empty collections (vs reduce)

  // not interested in results
  val done = possiblyEmpty.runWith(printSink)

  val results = possiblyEmpty.runWith(Sink.seq)
  results.onComplete {
    case Success(results) =>
      logger.info(s"Successfully processed: ${results.size} elements")
      system.terminate()
    case Failure(exception) =>
      logger.info(s"The stream failed with: $exception}")
      system.terminate()
  }
}

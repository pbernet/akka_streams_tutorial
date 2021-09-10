package sample.stream_shared_state

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import org.slf4j.{Logger, LoggerFactory}

import java.time.{Instant, LocalDateTime, ZoneOffset}
import scala.collection.immutable._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Split time series data into sub-streams for each second
  * Similar to: [[SplitWhen]]
  * Similar streams operator: groupedWithin
  *
  * Inspired by:
  * https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/splitAfter.html
  *
  * Note that this implementation can be materialized many times because the
  * stateful decision is done in statefulMapConcat, see discussion:
  * https://discuss.lightbend.com/t/state-inside-of-flow-operators/5717
  */
object SplitAfter extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("SplitAfter")
  implicit val executionContext = system.dispatcher

  private def hasSecondChanged: () => Seq[(Int, Instant)] => Iterable[(Instant, Boolean)] = {
    () => {
      slidingElements => {
        if (slidingElements.size == 2) {
          val current = slidingElements.head
          val next = slidingElements.tail.head
          val currentBucket = LocalDateTime.ofInstant(current._2, ZoneOffset.UTC).withNano(0)
          val nextBucket = LocalDateTime.ofInstant(next._2, ZoneOffset.UTC).withNano(0)
          List((current._2, currentBucket != nextBucket))
        } else {
          val current = slidingElements.head
          List((current._2, false))
        }
      }
    }
  }

  val done = Source(1 to 100)
    .throttle(1, 100.millis)
    .map(elem => (elem, Instant.now()))
    .sliding(2)                           // allows to compare this element with the next element
    .statefulMapConcat(hasSecondChanged)  // stateful decision
    .splitAfter(_._2)                     // split when second has changed
    .map(_._1)                            // proceed with payload
    .fold(0)((acc, _) => acc + 1)   // sum
    .mergeSubstreams
    .runWith(Sink.foreach(each => println(s"Elements in group: $each")))

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


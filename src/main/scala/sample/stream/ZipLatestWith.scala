package sample.stream

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.ThrottleMode
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.DurationInt

/**
  * Filter elements from two sources, depending on a 'zipping function', inspired by:
  * https://stackoverflow.com/questions/74684285/iterating-over-two-source-and-filter-using-a-property-in-scala/74692534#74692534
  *
  * Doc zipLatestWith:
  * Whenever a new element appears, the zipping function is invoked with a tuple containing
  * the new element and the *last* seen element of the *other* stream
  *
  * With an unbounded stream, we get the expected reoccurring patterns:
  *
  * Record(A,1) vs Record(B,2)
  * -> Record(B,2) wins, Record(B,4) is the new element
  * Record(A,1) vs Record(B,4)
  * -> Record(B,4) wins, Record(A,3) AND Record(B,6) are the new elements
  * Record(A,3) vs Record(B,6)
  * -> Record(B,6) wins, Record(A,5) AND Record(B,2) are the new elements
  * Record(A,5) vs Record(B,2)
  * -> Record(A,5) wins, Record(A,1) AND Record(B,4) are the new elements
  * Record(A,1) vs Record(B,4)
  * -> Record(B,4) wins, Record(A,3) AND Record(B,6) are the new elements
  * Record(A,3) vs Record(B,6)
  * -> Record(B,6) wins, Record(A,5) AND Record(B,2) are the new elements
  * Record(A,5) vs Record(B,2)
  * -> Record(A,5) wins, Record(A,1) AND Record(B,4) are the new elements
  * Record(A,1) vs Record(B,4)
  * -> Record(B,4) ...
  *
  *
  * However, with a bounded stream (eg a take(10) on the sources) we get puzzling results:
  *
  * Record(A,1) vs Record(B,2)
  * -> Record(B,2) wins, Record(A,3) is the new element
  * Record(A,3) vs Record(B,2)
  * -> Record(A,3) wins, Record(B,4) is the new element
  * Record(A,3) vs Record(B,4)
  * -> Record(B,4) wins, Record(A,5) is the new element
  * Record(A,5) vs Record(B,6)
  * -> Record(B,6) wins, Record(A,1) is the new element
  * Record(A,1) vs Record(B,6)
  * -> Record(B,6) wins, Record(A,3) AND Record(B,2) are the new elements
  * Record(A,3) vs Record(B,2)
  * -> Record(A,3) wins, Record(A,5) is the new element
  * Record(A,5) vs Record(B,2)
  * -> Record(A,5) wins, Record(B,4) is the new element
  * Record(A,1) vs Record(B,4)
  * -> Record(B,4) wins, Record(A,3) is the new element
  * Record(A,3) vs Record(B,4)
  * -> Record(B,4) wins, Record(A,5) AND Record(B,6) are the new elements
  * Record(A,5) vs Record(B,6)
  * -> Record(B,6) wins, Record(A,1) is the new element
  * Record(A,1) vs Record(B,6)
  * -> Record(B,6)...
  */
object ZipLatestWith extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  case class Record(id: String, version: Integer)

  val printSink = Sink.foreach[Record](winner => logger.info(s"                          -> $winner"))

  // Simulate an unbounded stream, note that the use of 'take' influences the result
  val sourceA = Source.cycle(() => List(Record("A", 1), Record("A", 3), Record("A", 5)).iterator) //.take(10)
  val sourceB = Source.cycle(() => List(Record("B", 2), Record("B", 4), Record("B", 6)).iterator) //.take(10)
  val latestCombinedSource: Source[Record, _] =
    sourceA.zipLatestWith(sourceB) { (a, b) =>
      logger.info(s"$a vs $b")
      if (a.version > b.version) a else b
    }
  latestCombinedSource
    .throttle(1, 1.second, 10, ThrottleMode.shaping)
    .runWith(printSink)
}
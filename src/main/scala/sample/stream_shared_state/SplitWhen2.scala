package sample.stream_shared_state

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.IOResult
import org.apache.pekko.stream.scaladsl.{FileIO, Flow, Framing, Keep, Sink, Source}
import org.apache.pekko.util.ByteString
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.Paths
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Given a sorted stream of records e.g. from a file that has records sorted based on key
  *
  * key, value
  * 1,2
  * 1,3
  * 1,4
  * 1,5
  * 2,3  <--
  * 2,4
  * 2,5
  * 3,4  <--
  * 3,5
  * 4,5  <--
  *
  * Create a new group and collect records while the record key is the same as before.
  * When we encounter a changed key, emit the aggregation for the previous group and create a new group.
  *
  * Inspired by:
  * https://discuss.lightbend.com/t/groupwhile-on-akka-streams/5592/3
  *
  * and by doc:
  * https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/splitWhen.html
  *
  * Note that this implementation can be materialized many times because the
  * stateful decision is done in statefulMapConcat, see discussion:
  * https://discuss.lightbend.com/t/state-inside-of-flow-operators/5717
  *
  * SplitWhen2 uses statefulMap instead of statefulMapConcat (= deprecated)
  */
object SplitWhen2 {
  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    implicit val system: ActorSystem = ActorSystem()

    import system.dispatcher

    case class Record(key: String, value: String)

    val nonLinearCapacityFactor = 5 // raise to see how it scales
    val filename = "splitWhen.csv"

    def genResourceFile() = {
      logger.info(s"Writing resource file: $filename...")

      def fileSink(filename: String): Sink[String, Future[IOResult]] =
        Flow[String]
          .map(s => ByteString(s + System.lineSeparator))
          .toMat(FileIO.toPath(Paths.get(filename)))(Keep.right)

      Source.fromIterator(() => (1 to nonLinearCapacityFactor).toList.combinations(2))
        .map(each => s"${each.head},${each.last}")
        .runWith(fileSink(filename))
    }

    val sourceOfLines = FileIO.fromPath(Paths.get(filename))
      .via(Framing.delimiter(ByteString(System.lineSeparator), maximumFrameLength = 1024, allowTruncation = true)
        .map(_.utf8String))

    val csvToRecord: Flow[String, Record, NotUsed] = Flow[String]
      .map(_.split(",").map(_.trim))
      .map(cols => Record(cols(0), cols(1)))

    val terminationHook: Flow[Record, Record, Unit] = Flow[Record]
      .watchTermination { (_, done) =>
        done.onComplete {
          case Failure(err) => logger.info(s"Flow failed: $err")
          case _ => system.terminate(); logger.info(s"Flow terminated")
        }
      }

    val groupByKeyChange = {
      Flow[Record].statefulMap(() => List.empty[Record])(
          (stateList, nextElem) => {
            val newStateList = stateList :+ nextElem
            val lastElem = if (stateList.isEmpty) nextElem else stateList.reverse.head

            if (lastElem.key.equals(nextElem.key)) { // stateful decision
              // (list for next iteration, list of output elements)
              (newStateList, Nil)
            }
            else {
              // (list for next iteration, list of output elements)
              (List(nextElem), stateList)
            }
          },
          // Cleanup function, we return the last stateList
          stateList => Some(stateList))
        .filterNot(list => list.isEmpty)
    }

    val printSink = Sink.foreach[List[Record]](each => println(s"Reached sink: $each"))

    genResourceFile().onComplete {
      case Success(_) =>
        logger.info(s"Start processing...")
        sourceOfLines
          .via(csvToRecord)
          .via(terminationHook)
          .via(groupByKeyChange)

          .runWith(printSink)
      case Failure(exception) => logger.info(s"Exception: $exception")
    }

  }
}

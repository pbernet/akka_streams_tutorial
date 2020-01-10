package sample.stream

import java.nio.file.Paths

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, Flow, Framing, Keep, Sink, Source}
import akka.stream.{IOResult, SubstreamCancelStrategy}
import akka.util.ByteString

import scala.concurrent.Future
import scala.util.{Failure, Success}


/**
  * Given a sorted stream of records e.g. read a file that has records sorted based on key
  * Create a new Group / Subflow and collect records while the record key is the same as before (predicate),
  * once we encounter a new key, emit the aggregation for the previous group and create a new group.
  *
  * Inspired by:
  * https://discuss.lightbend.com/t/groupwhile-on-akka-streams/5592/3
  */
object SplitWhen extends App {
  implicit val system = ActorSystem("SplitWhen")
  implicit val executionContext = system.dispatcher

  val capacity = 1000
  val filename = "splitWhen.csv"

  def genResourceFile() = {

    def fileSink(filename: String): Sink[String, Future[IOResult]] =
      Flow[String]
        .map(s => ByteString(s + "\n"))
        .toMat(FileIO.toPath(Paths.get(filename)))(Keep.right)

    Source.fromIterator(() => (1 to capacity).toList.combinations(2))
      .map(each => s"${each.head},${each.last}")
      .runWith(fileSink(filename))
  }

  val sourceOfLines = FileIO.fromPath(Paths.get(filename))
    .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024, allowTruncation = true)
      .map(_.utf8String))

  val csvToRecord: Flow[String, Record, NotUsed] = Flow[String]
    .map(_.split(",").map(_.trim))
    .map(stringArrayToRecord)

  def stringArrayToRecord(cols: Array[String]) = Record(cols(0), cols(1))

  genResourceFile().onComplete {
    case Success(_) =>
      val done = sourceOfLines
        //.throttle(100, 1.second)
        .via(csvToRecord)
        .splitWhen(SubstreamCancelStrategy.drain) {
          var lastKey: Option[String] = None
          item =>
            lastKey match {
              case Some(item.key) | None =>
                lastKey = Some(item.key)
                false
              case _ =>
                lastKey = Some(item.key)
                true
            }
        }
        .fold(Vector.empty[Record])(_ :+ _)
        .mergeSubstreams
        .runForeach(println)
      terminateWhen(done)
  }

  case class Record(key: String, value: String)

  def terminateWhen(done: Future[_]) = {
    done.onComplete {
      case Success(b) =>
        println("Flow Success. About to terminate...")
        system.terminate()
      case Failure(e) =>
        println(s"Flow Failure: ${e.getMessage}. About to terminate...")
        system.terminate()
    }
  }
}
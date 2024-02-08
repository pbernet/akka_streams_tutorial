package sample.stream

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{FileIO, Flow, Framing, Sink, Source}
import org.apache.pekko.util.ByteString

import java.nio.file.Paths
import scala.util.{Failure, Success}

/**
  * Similar to: [[OneBillionRowChallenge]]
  * File reading is still sequential, but now:
  *  - the stream is partitioned, see batchSize
  *  - the aggregation is parallel, see parallelisationFactor
  *
  * Runtime is still around 4 minutes on i7-11850H,
  * so still nothing to write home about...
  */
object OneBillionRowChallengeScale extends App {
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val batchSize = 1024 * 1000 * 10 // 10MB
  val parallelisationFactor = 16 // Increase to utilize machine cores

  // Wire the generated 1 Billion records resource file
  val sourceOfRows = FileIO.fromPath(Paths.get("measurements_subset_10000.txt"), chunkSize = 1024 * 1000)
    .via(Framing.delimiter(ByteString(System.lineSeparator), maximumFrameLength = 256, allowTruncation = true)
      .map(_.utf8String))

  val csvToMeasurement: Flow[String, Measurement, NotUsed] = Flow[String]
    .map(_.split(";"))
    .map(cols => Measurement(cols(0), cols(1).toFloat))

  val aggregate: Flow[Measurement, MeasurementAggregate, NotUsed] =
    Flow[Measurement]
      // maxSubstreams must be larger than the number of locations in the file
      .groupBy(420, _.location, allowClosedSubstreamRecreation = true)
      .fold(MeasurementAggregate("", 0, 0, 0, 0)) {
        (ma: MeasurementAggregate, m: Measurement) =>
          val count = ma.count + 1
          val totalTemp = ma.totalTemp + m.temperature
          val minTemp = Math.min(ma.minTemp, m.temperature)
          val maxTemp = Math.max(ma.maxTemp, m.temperature)
          MeasurementAggregate(m.location, count, totalTemp, minTemp, maxTemp)
      }
      .mergeSubstreams

  private def aggregateFlow(eachSeq: Seq[String]) = {
    Source(eachSeq)
      .via(csvToMeasurement)
      .via(aggregate)
      .runWith(Sink.seq)
  }

  val result = sourceOfRows
    .grouped(batchSize)
    .mapAsync(parallelisationFactor)(batchSeq => aggregateFlow(batchSeq))
    .mapConcat(identity) // flatten
    .groupBy(420, _.location, allowClosedSubstreamRecreation = true)
    .fold(MeasurementAggregate("", 0, 0, 0, 0)) {
      (agg: MeasurementAggregate, each: MeasurementAggregate) =>
        val count = agg.count + each.count
        val totalTemp = agg.totalTemp + each.totalTemp
        val minTemp = Math.min(agg.minTemp, each.minTemp)
        val maxTemp = Math.max(agg.maxTemp, each.maxTemp)
        MeasurementAggregate(each.location, count, totalTemp, minTemp, maxTemp)
    }
    .mergeSubstreams
    .runWith(Sink.seq)

  result.onComplete {
    case Success(seq: Seq[MeasurementAggregate]) =>
      print("Sorted results from all parallel processes: \n")
      seq.toList.sortWith(_.location < _.location).foreach(println)
      println(s"Run with: " + Runtime.getRuntime.availableProcessors + " cores")
      println("Flow Success. About to terminate...")
      system.terminate()
    case Failure(e) =>
      println(s"Flow Failure: ${e.getMessage}. About to terminate...")
      system.terminate()
  }

  case class Measurement(location: String, temperature: Float)

  case class MeasurementAggregate(location: String, count: Int, totalTemp: Float, minTemp: Float, maxTemp: Float) {
    override def toString = s"Location: $location: AVG: ${totalTemp / count} MIN: $minTemp MAX: $maxTemp"
  }
}

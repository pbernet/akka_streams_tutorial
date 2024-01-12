package sample.stream

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{FileIO, Flow, Framing, Sink, Source}
import org.apache.pekko.util.ByteString

import java.nio.file.Paths
import scala.util.{Failure, Success}

/**
  * Similar to: [[OneBillionRowChallenge]]
  * The file reading is still sequential, but the aggregation is parallel
  * The file reading and the subsequent grouping is still the bottleneck
  * Runtime is around 4 minutes on i7-11850H, still nothing to write home about...
  *
  */
object OneBillionRowChallengeScale extends App {
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val batchSize = 1000 // Raise for large files
  val parallelisationFactor = 4 // Increase to utilize machine CPU

  // Wire generated 1 Billion records resource file
  val sourceOfRows = FileIO.fromPath(Paths.get("measurements_subset_10000.txt"), chunkSize = 100 * 1024)
    .via(Framing.delimiter(ByteString(System.lineSeparator), maximumFrameLength = 256, allowTruncation = true)
      .map(_.utf8String))

  def stringArrayToMeasurement(cols: Array[String]) = Measurement(cols(0), cols(1).toFloat)

  val csvToMeasurement: Flow[String, Measurement, NotUsed] = Flow[String]
    .map(_.split(";"))
    .map(stringArrayToMeasurement)

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

  val intermediateResult = sourceOfRows
    .grouped(batchSize)
    .mapAsync(parallelisationFactor)(eachSeq => aggregateFlow(eachSeq))
    .mapConcat(identity) // flatten
    .runWith(Sink.seq)

  // TODO Reduce the MeasurementAggregate(s) to obtain the final result
  intermediateResult.onComplete {
    case Success(seq: Seq[MeasurementAggregate]) =>
      print("Results from all parallel processes: \n")
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

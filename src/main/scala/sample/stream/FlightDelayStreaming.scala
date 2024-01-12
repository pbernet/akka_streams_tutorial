package sample.stream

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl._
import org.apache.pekko.util.ByteString
import org.apache.pekko.{Done, NotUsed}
import sample.graphstage.ThroughputMonitor
import sample.graphstage.ThroughputMonitor.Stats

import java.nio.file.Paths
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

/**
  * Inspired by:
  * https://blog.redelastic.com/diving-into-akka-streams-2770b3aeabb0
  *
  * Features:
  *  - aggregates flight delay records per carrier from a csv file
  *  - uses akka stream operators (no graphs)
  *  - shows "stateful processing" with reduce-by-key, see
  *    https://doc.akka.io/docs/akka/current/stream/stream-cookbook.html#implementing-reduce-by-key
  *  - uses [[sample.graphstage.ThroughputMonitor]] to collect benchmarks
  *
  * For testing, we use the shortened csv data file: 2008_subset.csv
  *
  * Micro benchmarks:
  *
  * Download csv file with data from 2008 (689MB, 7'009'729 records):
  * http://stat-computing.org/dataexpo/2009/the-data.html
  * and store locally, eg to src/main/resources/2008.csv
  *
  * Typical results on 2012 vintage MacBook Pro with 8 cores
  * with default dispatcher and default JVM param:
  * JDK 11.0.11    41 seconds (168905/sec)
  * JDK 17.0.2     33 seconds (208370/sec)
  * JDK 19.0.1     29 seconds (240551/sec)
  * graalvm-ce-19  28 seconds (249179/sec)
  * graalvm-jdk-21 27 seconds (253324/sec)
  *
  * Doc:
  * https://fullgc.github.io/how-to-tune-akka-to-get-the-most-from-your-actor-based-system-part-1
  * https://letitcrash.com/post/20397701710/50-million-messages-per-second-on-a-single
  * https://shekhargulati.com/2017/09/30/understanding-akka-dispatchers/
  */
object FlightDelayStreaming extends App {
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val sourceOfLines = FileIO.fromPath(Paths.get("src/main/resources/2008.csv"))
    .via(Framing.delimiter(ByteString(System.lineSeparator), maximumFrameLength = 1024, allowTruncation = true)
      .map(_.utf8String))

  // Split csv into a string array and transform each array into a FlightEvent
  val csvToFlightEvent: Flow[String, FlightEvent, NotUsed] = Flow[String]
    .map(_.split(",").map(_.trim))
    .map(stringArrayToFlightEvent)
  // A custom dispatcher with fork/join executor may help on certain machines
  //.withAttributes(ActorAttributes.dispatcher("custom-dispatcher-fork-join"))

  def stringArrayToFlightEvent(cols: Array[String]) = FlightEvent(cols(0), cols(1), cols(2), cols(3), cols(4), cols(5), cols(6), cols(7), cols(8), cols(9), cols(10), cols(11), cols(12), cols(13), cols(14), cols(15), cols(16), cols(17), cols(18), cols(19), cols(20), cols(21), cols(22), cols(23), cols(24), cols(25), cols(26), cols(27), cols(28))

  // Transform FlightEvent to FlightDelayRecord (only for records with a delay)
  // Note that with this approach the average is calculated across delayed flights only
  val filterAndConvert: Flow[FlightEvent, FlightDelayRecord, NotUsed] =
  Flow[FlightEvent]
    .filter(r => Try(r.arrDelayMins.toInt).getOrElse(-1) > 0) // convert arrival delays to ints, filter out non delays
    .mapAsyncUnordered(parallelism = 2) { r =>
      Future(FlightDelayRecord(r.year, r.month, r.dayOfMonth, r.flightNum, r.uniqueCarrier, r.arrDelayMins))
      }

  // Aggregate number of delays and totalMins
  val averageCarrierDelay: Flow[FlightDelayRecord, FlightDelayAggregate, NotUsed] =
    Flow[FlightDelayRecord]
      // maxSubstreams must be larger than the number of UniqueCarrier in the file
      .groupBy(30, _.uniqueCarrier, allowClosedSubstreamRecreation = true)
      //.wireTap(each => println(s"Processing FlightDelayRecord: $each"))
      .fold(FlightDelayAggregate("", 0, 0)) {
        (x: FlightDelayAggregate, y: FlightDelayRecord) =>
          val count = x.count + 1
          val totalMins = x.totalMins + Try(y.arrDelayMins.toInt).getOrElse(0)
          FlightDelayAggregate(y.uniqueCarrier, count, totalMins)
      }
      .mergeSubstreams

  def averageSink[A](avg: A): Unit = {
    avg match {
      case aggregate@FlightDelayAggregate(_, _, _) => println(aggregate)
      case x => println("no idea what " + x + "is!")
    }
  }

  val sink: Sink[FlightDelayAggregate, Future[Done]] = Sink.foreach(averageSink[FlightDelayAggregate])

  var stats: List[Stats] = Nil


  val done = sourceOfLines
    .via(ThroughputMonitor(1000.millis, { st => stats = st :: stats }))
    .via(csvToFlightEvent)
    .via(filterAndConvert)
    .via(averageCarrierDelay)
    .runWith(sink)

  terminateWhen(done)

  def terminateWhen(done: Future[Done]) = {
    done.onComplete {
      case Success(_) =>
        println(s"Run with: " + Runtime.getRuntime.availableProcessors + " cores")
        ThroughputMonitor.avgThroughputReport(stats)
        println("Flow Success. About to terminate...")
        system.terminate()
      case Failure(e) =>
        println(s"Flow Failure: ${e.getMessage}. About to terminate...")
        system.terminate()
    }
  }
}

case class FlightEvent(
                        year: String,
                        month: String,
                        dayOfMonth: String,
                        dayOfWeek: String,
                        depTime: String,
                        scheduledDepTime: String,
                        arrTime: String,
                        scheduledArrTime: String,
                        uniqueCarrier: String,
                        flightNum: String,
                        tailNum: String,
                        actualElapsedMins: String,
                        crsElapsedMins: String,
                        airMins: String,
                        arrDelayMins: String,
                        depDelayMins: String,
                        originAirportCode: String,
                        destinationAirportCode: String,
                        distanceInMiles: String,
                        taxiInTimeMins: String,
                        taxiOutTimeMins: String,
                        flightCancelled: String,
                        cancellationCode: String, // (A = carrier, B = weather, C = NAS, D = security)
                        diverted: String, // 1 = yes, 0 = no
                        carrierDelayMins: String,
                        weatherDelayMins: String,
                        nasDelayMins: String,
                        securityDelayMins: String,
                        lateAircraftDelayMins: String)

case class FlightDelayRecord(
                              year: String,
                              month: String,
                              dayOfMonth: String,
                              flightNum: String,
                              uniqueCarrier: String,
                              arrDelayMins: String) {
  override def toString = s"$year/$month/$dayOfMonth - $uniqueCarrier $flightNum - $arrDelayMins"
}

case class FlightDelayAggregate(uniqueCarrier: String, count: Int, totalMins: Int) {
  override def toString = s"Delays for carrier $uniqueCarrier: ${Try(totalMins / count).getOrElse(0)} average mins, $count delayed flights"
}
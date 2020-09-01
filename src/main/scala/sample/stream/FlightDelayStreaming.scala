package sample.stream

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.util.ByteString
import akka.{Done, NotUsed}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
  * Inspired by:
  * https://blog.redelastic.com/diving-into-akka-streams-2770b3aeabb0
  *
  * Features:
  *  - reads large file in streaming fashion (here with subset)
  *  - uses akka stream operators (no graph)
  *  - shows "stateful processing" with reduce-by-key
  *
  * Doc reduce-by-key:
  * https://doc.akka.io/docs/akka/current/stream/stream-cookbook.html#implementing-reduce-by-key
  *
  * Download (large) flight data files:
  * https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=
  * and store locally, eg to src/main/resources/myData.csv
  */
object FlightDelayStreaming extends App {
  implicit val system = ActorSystem("FlightDelayStreaming")
  implicit val executionContext = system.dispatcher

  val sourceOfLines = FileIO.fromPath(Paths.get("src/main/resources/2008_subset.csv"))
    .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024, allowTruncation = true)
      .map(_.utf8String))

  // Split csv into a string array and transform each array into a FlightEvent
  val csvToFlightEvent: Flow[String, FlightEvent, NotUsed] = Flow[String]
    .map(_.split(",").map(_.trim))
    .map(stringArrayToFlightEvent)

  def stringArrayToFlightEvent(cols: Array[String]) = FlightEvent(cols(0), cols(1), cols(2), cols(3), cols(4), cols(5), cols(6), cols(7), cols(8), cols(9), cols(10), cols(11), cols(12), cols(13), cols(14), cols(15), cols(16), cols(17), cols(18), cols(19), cols(20), cols(21), cols(22), cols(23), cols(24), cols(25), cols(26), cols(27), cols(28))

  // Transform FlightEvent to FlightDelayRecord (only for records with a delay)
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
      // on large dataset you may set allowClosedSubstreamRecreation to true
      .groupBy(30, _.uniqueCarrier, allowClosedSubstreamRecreation = false)
      .wireTap(each => println(s"Processing FlightDelayRecord: $each"))
      .fold(FlightDelayAggregate("", 0, 0)) {
        (x: FlightDelayAggregate, y: FlightDelayRecord) =>
          val count = x.count + 1
          val totalMins = x.totalMins + Try(y.arrDelayMins.toInt).getOrElse(0)
          FlightDelayAggregate(y.uniqueCarrier, count, totalMins)
      }
      .async  //for maximizing throughput
      .mergeSubstreams

  def averageSink[A](avg: A): Unit = {
    avg match {
      case aggregate@FlightDelayAggregate(_,_,_) => println(aggregate)
      case x => println("no idea what " + x + "is!")
    }
  }

  val sink: Sink[FlightDelayAggregate, Future[Done]] = Sink.foreach(averageSink[FlightDelayAggregate])

  val done = sourceOfLines
    .via(csvToFlightEvent)
    .via(filterAndConvert)
    .via(averageCarrierDelay)
    .runWith(sink)

  terminateWhen(done)

  def terminateWhen(done: Future[Done]) = {
    done.onComplete {
      case Success(_) =>
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
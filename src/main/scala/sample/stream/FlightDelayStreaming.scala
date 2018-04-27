package sample.stream

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import akka.{Done, NotUsed}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
  Inspired by:
  https://blog.redelastic.com/diving-into-akka-streams-2770b3aeabb0
  but with regular processing stages (= no graph)

  Download flight data:
  http://stat-computing.org/dataexpo/2009/the-data.html
  and store locally, eg src/main/resources/2008.csv
  */
object FlightDelayStreaming {
  implicit val system = ActorSystem("FlightDelayStreaming")
  implicit val executionContext = system.dispatcher
  implicit val materializer = ActorMaterializer()

  def main(args: Array[String]): Unit = {
    val sourceOfLines = FileIO.fromPath(Paths.get("src/main/resources/2008.csv"))
      .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024, allowTruncation = true)
      .map(_.utf8String))
    val sink = Sink.foreach(averageSink)

    val done = sourceOfLines
      .via(csvToFlightEvent)
      .via(filterAndConvert)
      .via(averageCarrierDelay)
      .runWith(sink)

    terminate(done)
  }

  // Split csv into a string array and transform each array into a FlightEvent
  val csvToFlightEvent: Flow[String, FlightEvent, NotUsed] = Flow[String]
    .map(_.split(",").map(_.trim)) // we now have our columns split by ","
    .map(stringArrayToFlightEvent) // we convert an array of columns to a FlightEvent

  def stringArrayToFlightEvent(cols: Array[String]) = FlightEvent(cols(0), cols(1), cols(2), cols(3), cols(4), cols(5), cols(6), cols(7), cols(8), cols(9), cols(10), cols(11), cols(12), cols(13), cols(14), cols(15), cols(16), cols(17), cols(18), cols(19), cols(20), cols(21), cols(22), cols(23), cols(24), cols(25), cols(26), cols(27), cols(28))

  // transform FlightEvents to DelayRecords (only for records with a delay)
  val filterAndConvert: Flow[FlightEvent, FlightDelayRecord, NotUsed] =
    Flow[FlightEvent]
      .filter(r => Try(r.arrDelayMins.toInt).getOrElse(-1) > 0) // convert arrival delays to ints, filter out non delays
      .mapAsyncUnordered(parallelism = 2) { r =>
      Future(FlightDelayRecord(r.year, r.month, r.dayOfMonth, r.flightNum, r.uniqueCarrier, r.arrDelayMins))
    }

  val averageCarrierDelay: Flow[FlightDelayRecord, (String, Int, Int), NotUsed] =
    Flow[FlightDelayRecord]
      .groupBy(30, _.uniqueCarrier) //maxSubstreams must be larger than the number of carriers in the file
      .map { each => println(s"Processing FlightDelayRecord: $each"); each }
      .fold(("", 0, 0)) {
        (x: (String, Int, Int), y: FlightDelayRecord) =>
          val count = x._2 + 1
          val totalMins = x._3 + Try(y.arrDelayMins.toInt).getOrElse(0)
          (y.uniqueCarrier, count, totalMins)
      }
      .mergeSubstreams

  def averageSink[A](avg: A): Unit = {
    avg match {
      case (a: String, b: Int, c: Int) => println(s"Delays for carrier $a: ${Try(c / b).getOrElse(0)} average mins, $b delayed flights")
      case x => println("no idea what " + x + "is!")
    }
  }

  def terminate(done: Future[Done]) = {
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
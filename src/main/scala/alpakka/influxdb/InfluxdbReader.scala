package alpakka.influxdb

import com.influxdb.LogLevel
import com.influxdb.client.InfluxDBClientFactory
import com.influxdb.client.scala.InfluxDBClientScalaFactory
import com.influxdb.query.FluxTable
import com.influxdb.query.dsl.Flux
import com.influxdb.query.dsl.functions.restriction.Restrictions
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Supervision
import org.slf4j.{Logger, LoggerFactory}

import java.time.temporal.ChronoUnit
import java.util
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal;

/**
  *
  * Doc read data via akka-streams via Scala client:
  * https://github.com/influxdata/influxdb-client-java/tree/master/client-scala
  *
  * Doc Flux query lang:
  * https://docs.influxdata.com/influxdb/v2.0/query-data/get-started
  *
  * Doc flux-dsl
  * https://github.com/influxdata/influxdb-client-java/tree/master/flux-dsl
  *
  */
class InfluxdbReader(baseURL: String, token: String, org: String = "testorg", bucket: String = "testbucket", actorSystem: ActorSystem) {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  implicit val system: ActorSystem = actorSystem
  implicit val ec: ExecutionContextExecutor = actorSystem.dispatcher

  //import system.dispatcher

  val deciderFlow: Supervision.Decider = {
    case NonFatal(e) =>
      logger.info(s"Stream failed with: ${e.getMessage}, going to restart")
      Supervision.Restart
    case _ => Supervision.Stop
  }

  private val influxDBClient = InfluxDBClientFactory.create(baseURL, token.toCharArray, org, bucket).setLogLevel(LogLevel.BASIC)
  private val influxdbClientScala = InfluxDBClientScalaFactory.create(baseURL, token.toCharArray, org)

  private val query =
    """
      interval = 1m

      from(bucket: "testbucket")
         |> range(start: -interval)
      """

  // TODO Activate, when "com.influxdb" %% "influxdb-client-scala" is available for pekko
  //    def source() = influxdbClientScala
  //      .getQueryScalaApi()
  //      .query(query)

  // TODO Activate, when "com.influxdb" %% "influxdb-client-scala" is available for pekko
  def getQuerySync(mem: String) = {
    //      logger.info(s"Query raw for measurements of type: $mem")
    //      val result = source()
    //        .filter(fluxRecord => fluxRecord.getMeasurement().equals(mem) )
    //        .wireTap(fluxRecord => {
    //          val measurement = fluxRecord.getMeasurement()
    //          val value = fluxRecord.getValue()
    //          logger.debug(s"About to process measurement: $measurement with value: $value")
    //        })
    //        .withAttributes(ActorAttributes.supervisionStrategy(deciderFlow))
    //        .runWith(Sink.seq)
    //
    //      Await.result(result, 10.seconds)
  }

  def fluxQueryCount(mem: String): Long = {
    logger.info(s"Query with flux DSL for measurements of type: $mem")
    val flux = Flux
      .from(bucket)
      .range(-1L, ChronoUnit.MINUTES)
      .filter(Restrictions.measurement().equal(mem))
      .count()
    val out: util.List[FluxTable] = influxDBClient.getQueryApi().query(flux.toString)
    if (out.isEmpty || out.get(0).getRecords.isEmpty) 0
    else out.size()
  }

  def run(): Unit = {
    system.scheduler.scheduleWithFixedDelay(10.second, 10.seconds)(() =>
      logger.info("Records for this period: {}", fluxQueryCount("testMem")))
  }

  def shutdown() = {
    influxDBClient.close()
    influxdbClientScala.close()
  }
}

case class CompressedObservation(
                                  sensorID: String = "uninitialised", // per sensor
                                  traceID: String = "", // per measurement
                                  avgValue: Float = 0,
                                  minValue: Float = 0,
                                  maxValue: Float = 0,
                                  valueType: String = "",
                                  nPoints: Int = 0,
                                  startTimestamp: String = "",
                                  stopTimestamp: String = ""
                                ) {
  override def toString = s"For sensorID: $sensorID - nPoints: $nPoints between: $startTimestamp/$stopTimestamp avg value: $avgValue"
}
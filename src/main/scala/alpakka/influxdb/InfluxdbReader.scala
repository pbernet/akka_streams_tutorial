package alpakka.influxdb

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.{ActorAttributes, Supervision}
import com.influxdb.LogLevel
import com.influxdb.client.InfluxDBClientFactory
import com.influxdb.client.scala.InfluxDBClientScalaFactory
import com.influxdb.query.FluxTable
import com.influxdb.query.dsl.Flux
import com.influxdb.query.dsl.functions.restriction.Restrictions
import org.slf4j.{Logger, LoggerFactory}

import java.time.temporal.ChronoUnit
import java.util
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal

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

  implicit val system = actorSystem
  implicit val ec = actorSystem.dispatcher

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
    interval = 10m

    from(bucket: "testbucket")
       |> range(start: -interval)
    """

  def source() = influxdbClientScala
    .getQueryScalaApi()
    .query(query)

  def getQuerySync(mem: String) = {
    logger.info("About to query with with stream result")
    val result = source()
      .filter(fluxRecord => fluxRecord.getMeasurement().equals(mem) )
      .wireTap(fluxRecord => {
        val measurement = fluxRecord.getMeasurement()
        val value = fluxRecord.getValue()
        logger.info(s"About to process measurement: $measurement with value: $value")
      })
      .withAttributes(ActorAttributes.supervisionStrategy(deciderFlow))
      .runWith(Sink.seq)

    Await.result(result, 10.seconds)
  }

  def fluxQueryCount(mem: String) : Long = {
    logger.info(s"About to query with flux DSL for measurements of type: $mem")
    val flux = Flux
      .from(this.bucket)
      .range(-1L, ChronoUnit.DAYS)
      .filter(Restrictions.measurement().equal(mem))
      .count()
    val out: util.List[FluxTable] = this.influxDBClient.getQueryApi().query(flux.toString)
    if (out.isEmpty || out.get(0).getRecords.isEmpty) 0
    else out.size()
  }

  def shutdown() = {
    influxDBClient.close()
    influxdbClientScala.close()
  }
}
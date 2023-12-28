package alpakka.clickhousedb

import com.crobox.clickhouse.ClickhouseClient
import com.crobox.clickhouse.stream.{ClickhouseSink, Insert}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Framing, Sink, Source}
import org.apache.pekko.util.ByteString
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.Try


/**
  * DB access via the Scala client
  * Run with integration test: alpakka.clickhousedb.ClickhousedbIT
  *
  * We use the format `JSONEachRow`
  * https://clickhouse.com/docs/en/interfaces/formats#jsoneachrow
  * All possible formats:
  * https://clickhouse.com/docs/en/interfaces/formats
  */
class ClickhouseDB(httpPort: Int) {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  // Tweak config with mapped HTTP port from container
  val tweakedConf: Config = ConfigFactory.empty()
    .withValue("crobox.clickhouse.client.connection.port", ConfigValueFactory.fromAnyRef(httpPort))
    .withFallback(ConfigFactory.load())

  val client = new ClickhouseClient(Some[Config](tweakedConf))
  logger.info(s"Connected to server version: ${client.serverVersion}")

  def testRead(): String = {
    val result = Await.result(client.query("SELECT 1"), 10.seconds)
    logger.info(s"Got query result: $result")
    result.trim
  }

  def writeAll(noOfRecords: Integer) = {
    Source(1 to noOfRecords)
      .map(id => Insert("test.my_table", s"{\"myfloat_nullable\": $id, \"mystr\": $id, \"myint_id\": $id}"))
      .wireTap((insert: Insert) => logger.debug(s"Insert record with type JSONEachRow: $insert"))
      .runWith(ClickhouseSink.toSink(tweakedConf, client))
  }

  // The most intuitive way to read the streamed records
  def readAllSource() = {
    val resultFut = client.source("SELECT * FROM test.my_table ORDER BY myint_id ASC FORMAT JSONEachRow SETTINGS output_format_json_named_tuples_as_objects=1;")
      .wireTap((line: String) => logger.debug(s"Raw JSON record: $line"))
      .runWith(Sink.seq)

    resultFut.flatMap(each => Future(each.size))
  }

  // An alternative way to read, allows for more control, eg while massaging the result
  def readAllSourceByteString() = {
    val resultFut = client.sourceByteString("SELECT * FROM test.my_table ORDER BY myint_id ASC FORMAT JSONEachRow SETTINGS output_format_json_named_tuples_as_objects=1;")
      .wireTap((allLines: ByteString) => logger.debug("Raw JSON records all-in-one: \n" + allLines.utf8String))
      .via(Framing.delimiter(ByteString.fromString(System.lineSeparator()), 1024))
      .wireTap(eachLine => logger.debug(s"Raw JSON record: ${eachLine.utf8String}"))
      .runWith(Sink.seq)

    resultFut.flatMap(each => Future(each.size))
  }

  def countRows(): Int = {
    val resFut = client
      .query(s"SELECT COUNT(*) FROM test.my_table")
      .map(res => Try(res.stripLineEnd.toInt).getOrElse(0))
    Await.result(resFut, 10.seconds)
  }
}

object ClickhouseDB extends App {

  def apply(httpPort: Int) = new ClickhouseDB(httpPort: Int)
}
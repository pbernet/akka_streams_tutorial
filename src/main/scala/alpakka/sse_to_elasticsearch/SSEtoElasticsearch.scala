package alpakka.sse_to_elasticsearch

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.alpakka.elasticsearch.WriteMessage.createIndexMessage
import akka.stream.alpakka.elasticsearch._
import akka.stream.alpakka.elasticsearch.scaladsl.{ElasticsearchSink, ElasticsearchSource}
import akka.stream.scaladsl.{Flow, RestartSource, Sink, Source}
import akka.stream.{ActorAttributes, RestartSettings, Supervision}
import org.slf4j.{Logger, LoggerFactory}
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.utility.DockerImageName
import play.api.libs.json._
import spray.json.DefaultJsonProtocol.{jsonFormat8, _}
import spray.json.JsonFormat

import scala.concurrent.duration._
import scala.sys.process.Process
import scala.util.Try
import scala.util.control.NonFatal

/**
  * Read Wikipedia edits via SSE (like in [[alpakka.sse.SSEClientWikipediaEdits]])
  * and write them to Elasticsearch version 7.x server
  *
  * Runs now with Alpakka 3.x where Elasticsearch clients are based on akka-http
  *
  * Doc:
  * https://doc.akka.io/docs/alpakka/current/elasticsearch.html
  */
object SSEtoElasticsearch extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("SSEtoElasticsearch")
  implicit val executionContext = system.dispatcher
  val decider: Supervision.Decider = {
    case NonFatal(e) =>
      logger.warn(s"Stream failed with: $e, going to restart")
      Supervision.Restart
  }

  // Data bridge between SSE and Elasticsearch
  case class Change(timestamp: Long, serverName: String, user: String, cmdType: String, isBot: Boolean, isNamedBot: Boolean, lengthNew: Int = 0, lengthOld: Int = 0)

  implicit val format: JsonFormat[Change] = jsonFormat8(Change)

  val dockerImageName = DockerImageName
    .parse("docker.elastic.co/elasticsearch/elasticsearch-oss")
    .withTag("7.10.2")
  val elasticsearchContainer = new ElasticsearchContainer(dockerImageName)
  elasticsearchContainer.start()

  val address = elasticsearchContainer.getHttpHostAddress
  val connectionSettings = ElasticsearchConnectionSettings(s"http://$address")

  val indexName = "wikipediaedits"
  val elasticsearchParamsV7 = ElasticsearchParams.V7(indexName)
  val matchAllQuery = """{"match_all": {}}"""

  val sourceSettings = ElasticsearchSourceSettings(connectionSettings).withApiVersion(ApiVersion.V7)
  val elasticsearchSource = ElasticsearchSource
    .typed[Change](
      elasticsearchParamsV7,
      query = matchAllQuery,
      settings = sourceSettings
    )

  val sinkSettings =
    ElasticsearchWriteSettings(connectionSettings)
      .withBufferSize(10)
      .withVersionType("internal")
      .withRetryLogic(RetryAtFixedRate(maxRetries = 5, retryInterval = 1.second))
      .withApiVersion(ApiVersion.V7)
  val elasticsearchSink =
    ElasticsearchSink.create[Change](
      elasticsearchParamsV7,
      settings = sinkSettings
    )


  logger.info(s"Elasticsearch container listening on: ${elasticsearchContainer.getHttpHostAddress}")
  readFromWikipediaAndWriteToElasticsearch()

  // Wait for the index to populate
  Thread.sleep(10.seconds.toMillis)
  browserClient()
  queryOnce()


  private def readFromWikipediaAndWriteToElasticsearch() = {

    import akka.http.scaladsl.unmarshalling.sse.EventStreamUnmarshalling._

    val restartSettings = RestartSettings(1.second, 10.seconds, 0.2).withMaxRestarts(10, 1.minute)
    val restartSource = RestartSource.withBackoff(restartSettings) { () =>
      Source.futureSource {
        Http()
          .singleRequest(HttpRequest(
            uri = "https://stream.wikimedia.org/v2/stream/recentchange"
          ))
          .flatMap(Unmarshal(_).to[Source[ServerSentEvent, NotUsed]])
      }.withAttributes(ActorAttributes.supervisionStrategy(decider))
    }

    val parserFlow: Flow[ServerSentEvent, Change, NotUsed] = Flow[ServerSentEvent].map {
      event: ServerSentEvent => {

        def tryToInt(s: String) = Try(s.toInt).toOption.getOrElse(0)

        def isNamedBot(bot: Boolean, user: String): Boolean = {
          if (bot) user.toLowerCase().contains("bot") else false
        }

        val timestamp = (Json.parse(event.data) \ "timestamp").as[Long]

        val serverName = (Json.parse(event.data) \ "server_name").as[String]

        val user = (Json.parse(event.data) \ "user").as[String]

        val cmdType = (Json.parse(event.data) \ "type").as[String]

        val bot = (Json.parse(event.data) \ "bot").as[Boolean]

        if (cmdType == "new" || cmdType == "edit") {
          val lengthNew = (Json.parse(event.data) \ "length" \ "new").getOrElse(JsString("0")).toString()
          val lengthOld = (Json.parse(event.data) \ "length" \ "old").getOrElse(JsString("0")).toString()
          Change(timestamp, serverName, user, cmdType, isBot = bot, isNamedBot = isNamedBot(bot, user), tryToInt(lengthNew), tryToInt(lengthOld))
        } else {
          Change(timestamp, serverName, user, cmdType, isBot = bot, isNamedBot = isNamedBot(bot, user))
        }
      }
    }

    restartSource
      .via(parserFlow)
      .map(change => createIndexMessage(change))
      .wireTap(each => logger.info(s"Add to index: $each"))
      .runWith(elasticsearchSink)
  }

  private def browserClient() = {
    val os = System.getProperty("os.name").toLowerCase
    if (os == "mac os x") Process(s"open http://localhost:${elasticsearchContainer.getMappedPort(9200)}/$indexName/_search?q=*").!
  }

  private def queryOnce() = {
    for {
      result <- readFromElasticsearchTyped()
      resultRaw <- readFromElasticsearchRaw()
    } {
      logger.info(s"Read typed: ${result.size} records. 1st: ${result.head}")
      logger.info(s"Read raw: ${resultRaw.size} records. 1st: ${resultRaw.head}")
    }
  }

  private def readFromElasticsearchTyped() = {
    elasticsearchSource.runWith(Sink.seq)
  }

  private def readFromElasticsearchRaw() = {
    ElasticsearchSource
      .create(
        elasticsearchParamsV7,
        query = matchAllQuery,
        settings = sourceSettings
      ).runWith(Sink.seq)
  }
}

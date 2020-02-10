package alpakka.sse_to_elasticsearch

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.alpakka.elasticsearch.WriteMessage.createIndexMessage
import akka.stream.alpakka.elasticsearch.scaladsl.{ElasticsearchSink, ElasticsearchSource}
import akka.stream.alpakka.elasticsearch.{ElasticsearchWriteSettings, ReadResult, WriteMessage}
import akka.stream.scaladsl.{Flow, RestartSource, Sink, Source}
import akka.stream.{ActorAttributes, Supervision}
import akka.{Done, NotUsed}
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.slf4j.{Logger, LoggerFactory}
import org.testcontainers.elasticsearch.ElasticsearchContainer
import play.api.libs.json._
import spray.json
import spray.json.DefaultJsonProtocol.{jsonFormat8, _}
import spray.json.JsonFormat

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.sys.process.Process
import scala.util.Try
import scala.util.control.NonFatal

/**
  * Read Wikipedia edits via SSE (like in SSEClientWikipediaEdits) and write to Elasticsearch version 6.x
  *
  * Improvement:
  * - Add JSON directly to index without using type class [[Elasticsearch.Change]], Doc:
  *   https://doc.akka.io/docs/alpakka/current/elasticsearch.html#storing-documents-from-strings
  */
object Elasticsearch {
  implicit val system = ActorSystem("Elasticsearch")
  implicit val executionContext = system.dispatcher
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val decider: Supervision.Decider = {
    case NonFatal(e) =>
      logger.warn(s"Stream failed with: $e, going to restart")
      Supervision.Restart
  }

  case class Change(timestamp: Long, serverName: String, user: String, cmdType: String, isBot: Boolean, isNamedBot: Boolean, lengthNew: Int = 0, lengthOld: Int = 0)
  implicit val format: JsonFormat[Change] = jsonFormat8(Change)

  val elasticsearchVersion = "6.8.6"
  val elasticsearchContainer = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch-oss:" + elasticsearchVersion)
  elasticsearchContainer.start()
  val elasticsearchAddress: Array[String] = elasticsearchContainer.getHttpHostAddress.split(":")

  implicit val elasticSearchClient: RestClient = RestClient.builder(new HttpHost(elasticsearchAddress(0), elasticsearchAddress(1).toInt)).build()

  val indexName = "wikipediaedits"
  val typeName = "_doc"
  val elasticsearchSink: Sink[WriteMessage[Change, NotUsed], Future[Done]] = ElasticsearchSink.create[Change](indexName, typeName, ElasticsearchWriteSettings.create())
  val elasticsearchSource: Source[ReadResult[json.JsObject], NotUsed] = ElasticsearchSource.create(indexName, typeName, """{"match_all": {}}""")


  def main(args: Array[String]) {
    logger.info(s"Elasticsearch container listening on: ${elasticsearchContainer.getHttpHostAddress}")

    readFromWikipediaAndWriteToElasticsearch()

    Thread.sleep(10.seconds.toMillis)

    browserClient()

    for {
      result <- readFromElasticsearch(indexName, typeName)
      resultRaw <- readFromElasticsearchRaw(indexName, typeName)
    } {
      logger.info(s"Read: ${result.size} records. 1st: ${result.head}")
      logger.info(s"ReadRaw: ${resultRaw.size} records. 1st: ${resultRaw.head}")
    }
  }

  private def readFromElasticsearchRaw(indexName: String, typeName: String) = {

    val resultJson = elasticsearchSource
      //.wireTap(each => logger.info("Each: " + each))
      .map(_.source)
      .runWith(Sink.seq)
    resultJson
  }

  private def readFromElasticsearch(indexName: String, typeName: String) = {
    ElasticsearchSource
      .typed[Change](indexName, typeName, """{"match_all": {}}""")
      .map(_.source)
      .runWith(Sink.seq)
  }

  private def readFromWikipediaAndWriteToElasticsearch() = {

    import akka.http.scaladsl.unmarshalling.sse.EventStreamUnmarshalling._

    val restartSource = RestartSource.withBackoff(
      minBackoff = 3.seconds,
      maxBackoff = 30.seconds,
      randomFactor = 0.2 // adds 20% "noise" to vary the intervals slightly
    ) { () =>
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

    val done = restartSource
      .via(parserFlow)
      .map(change => createIndexMessage(change))
      .wireTap(each => logger.info(s"Add to index: $each"))
      .runWith(elasticsearchSink)
    done.onComplete { _ =>
      elasticSearchClient.close()
      elasticsearchContainer.stop()
    }
  }

  private def browserClient() = {
    val os = System.getProperty("os.name").toLowerCase
    if (os == "mac os x") Process(s"open http://localhost:${elasticsearchAddress(1).toInt}/$indexName/_search?q=*").!
  }
}

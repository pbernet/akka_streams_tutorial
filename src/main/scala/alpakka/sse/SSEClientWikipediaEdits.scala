package alpakka.sse

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.{Flow, RestartSource, Sink, Source}
import akka.stream.{ActorAttributes, RestartSettings, Supervision}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json._

import java.time.{Instant, ZoneId}
import scala.concurrent.duration._
import scala.sys.process._
import scala.util.Try
import scala.util.control.NonFatal


case class Change(timestamp: Long, serverName: String, user: String, cmdType: String, isBot: Boolean, isNamedBot: Boolean, lengthNew: Int = 0, lengthOld: Int = 0) {
  override def toString = {
    val localDateTime = Instant.ofEpochSecond(timestamp).atZone(ZoneId.systemDefault()).toLocalDateTime
    s"$localDateTime - $cmdType on server: $serverName by: $user isBot:$isBot isNamedBot:$isNamedBot new: $lengthNew old: $lengthOld (${lengthNew - lengthOld})"
  }
}

/**
  * Just because we can :-)
  * Consume the WikipediaEdits stream which is implemented with SSE - see:
  * https://wikitech.wikimedia.org/wiki/EventStreams
  * https://www.matthowlett.com/2017-12-23-exploring-wikipedia-ksql.html
  *
  */
object SSEClientWikipediaEdits extends App {
  implicit val system = ActorSystem("SSEClientWikipediaEdits")
  implicit val executionContext = system.dispatcher
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val decider: Supervision.Decider = {
    case NonFatal(e) =>
      logger.warn(s"Stream failed with: $e, going to restart")
      Supervision.Restart
  }

  browserClient()
  sseClient()

  private def browserClient() = {
    val os = System.getProperty("os.name").toLowerCase
    if (os == "mac os x") Process("open src/main/resources/SSEClientWikipediaEdits.html").!
  }

  private def sseClient() = {

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

    val printSink = Sink.foreach[Change] { each: Change => logger.info(each.toString()) }

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
      .runWith(printSink)
  }
}
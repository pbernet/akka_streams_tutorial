package alpakka.sse

import io.circe._
import io.circe.parser._
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.sse.ServerSentEvent
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse, Uri}
import org.apache.pekko.stream.connectors.sse.scaladsl.EventSource
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import org.apache.pekko.stream.{Supervision, ThrottleMode}
import org.slf4j.{Logger, LoggerFactory}

import java.time.{Instant, ZoneId}
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.sys.process._
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
  *
  * Uses Alpakka SSE client, Doc: https://doc.akka.io/docs/alpakka/current/sse.html
  * Similar usage in [[alpakka.sse_to_elasticsearch.SSEtoElasticsearch]])
  */
object SSEClientWikipediaEdits extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

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
    else if (os == "windows 10") Seq("cmd", "/c", "start src/main/resources/SSEClientWikipediaEdits.html").!
  }

  private def sseClient() = {
    val send: HttpRequest => Future[HttpResponse] = Http().singleRequest(_)

    val eventSource: Source[ServerSentEvent, NotUsed] =
      EventSource(
        uri = Uri("https://stream.wikimedia.org/v2/stream/recentchange"),
        send,
        None,
        retryDelay = 1.second
      )

    val printSink = Sink.foreach[Change] { each: Change => logger.info(each.toString()) }

    val parserFlow: Flow[ServerSentEvent, Change, NotUsed] = Flow[ServerSentEvent].map {
      event: ServerSentEvent => {

        def isNamedBot(bot: Boolean, user: String): Boolean = {
          if (bot) user.toLowerCase().contains("bot") else false
        }

        val cursor = parse(event.data).getOrElse(Json.Null).hcursor
        val timestamp: Long = cursor.get[Long]("timestamp").toOption.getOrElse(0)
        val serverName = cursor.get[String]("server_name").toOption.getOrElse("")
        val user = cursor.get[String]("user").toOption.getOrElse("")
        val cmdType = cursor.get[String]("type").toOption.getOrElse("")
        val bot = cursor.get[Boolean]("bot").toOption.getOrElse(false)

        if (cmdType == "new" || cmdType == "edit") {
          val length = cursor.downField("length")
          val lengthNew = length.get[Int]("new").toOption.getOrElse(0)
          val lengthOld = length.get[Int]("old").toOption.getOrElse(0)
          Change(timestamp, serverName, user, cmdType, isBot = bot, isNamedBot = isNamedBot(bot, user), lengthNew, lengthOld)
        } else {
          Change(timestamp, serverName, user, cmdType, isBot = bot, isNamedBot = isNamedBot(bot, user))
        }
      }
    }

    eventSource
      .throttle(elements = 1, per = 500.milliseconds, maximumBurst = 1, ThrottleMode.Shaping)
      .via(parserFlow)
      .runWith(printSink)
  }
}
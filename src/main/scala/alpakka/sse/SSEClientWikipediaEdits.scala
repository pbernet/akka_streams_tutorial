package alpakka.sse

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.{Flow, RestartSource, Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import alpakka.jms.ProcessingApp.logger
import play.api.libs.json._

import scala.concurrent.duration._
import scala.sys.process._
import scala.util.Try
import scala.util.control.NonFatal

/**
  * Just because we can :-)
  * Consume the WikipediaEdits stream which is implemented with SSE - see:
  * https://wikitech.wikimedia.org/wiki/EventStreams
  * https://www.matthowlett.com/2017-12-23-exploring-wikipedia-ksql.html
  *
  */
object SSEClientWikipediaEdits {
  val decider: Supervision.Decider = {
    case NonFatal(e) =>
      logger.warn(s"Stream failed with: ${e.getMessage}, going to restart")
      Supervision.Restart
  }
  implicit val system = ActorSystem("SSEClientWikipediaEdits")
  implicit val executionContext = system.dispatcher
  implicit val materializer = ActorMaterializer.create(ActorMaterializerSettings.create(system)
    .withDebugLogging(true)
    .withSupervisionStrategy(decider), system)

  def main(args: Array[String]) {
    browserClient()
    sseClient()
  }

  private def browserClient() = {
    val os = System.getProperty("os.name").toLowerCase
    if (os == "mac os x") Process("open ./src/main/resources/SSEClientWikipediaEdits.html").!
  }


  private def sseClient() = {

    import akka.http.scaladsl.unmarshalling.sse.EventStreamUnmarshalling._

    val restartSource = RestartSource.withBackoff(
      minBackoff = 3.seconds,
      maxBackoff = 30.seconds,
      randomFactor = 0.2 // adds 20% "noise" to vary the intervals slightly
    ) { () =>
      Source.fromFutureSource {
        Http()
          .singleRequest(HttpRequest(
            uri = "https://stream.wikimedia.org/v2/stream/recentchange"
          ))
          .flatMap(Unmarshal(_).to[Source[ServerSentEvent, NotUsed]])
      }
    }

    val printSink = Sink.foreach[Change] { each: Change => println(each.toString())}

    val parserFlow: Flow[ServerSentEvent, (String, String), NotUsed] = Flow[ServerSentEvent].map {
      event: ServerSentEvent => {
        val server_name = (Json.parse(event.data) \ "server_name").as[String]
        val user = (Json.parse(event.data) \ "user").as[String]
        (server_name, user)
      }
    }

    restartSource
      .via(parserFlow)
      .runWith(printSink)
  }
}
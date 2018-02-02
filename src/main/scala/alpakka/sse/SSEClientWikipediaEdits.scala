package alpakka.sse

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import play.api.libs.json._

import scala.concurrent.Future
import scala.sys.process._

/**
  * Just because we can :-)
  * Consume the WikipediaEdits stream which is implemented with SSE - see:
  * https://wikitech.wikimedia.org/wiki/EventStreams
  * https://www.matthowlett.com/2017-12-23-exploring-wikipedia-ksql.html
  *
  */
object SSEClientWikipediaEdits {
  implicit val system = ActorSystem("SSEClientWikipediaEdits")
  implicit val executionContext = system.dispatcher
  implicit val materializerServer = ActorMaterializer()

  def main(args: Array[String]) {
    browserClient()
    sseClient()
  }

  private def browserClient() = {
    val os = System.getProperty("os.name").toLowerCase
    if (os == "mac os x") "open ./src/main/scala/alpakka/sse/index.html".!
  }


  private def sseClient() = {

    import akka.http.scaladsl.unmarshalling.sse.EventStreamUnmarshalling._

    val sourceFuture: Future[Source[ServerSentEvent, NotUsed]] =
      Http()
        .singleRequest(HttpRequest(
          uri = "https://stream.wikimedia.org/v2/stream/recentchange"
        ))
        .flatMap(Unmarshal(_).to[Source[ServerSentEvent, NotUsed]])

    val printSink =
      Sink.foreach[(String, String)] { each: (String, String) =>
        println(s"Change on server: ${each._1} by: ${each._2}")
      }

    val parserFlow: Flow[ServerSentEvent, (String, String), NotUsed] = Flow[ServerSentEvent].map {
      event: ServerSentEvent => {
        val server_name = (Json.parse(event.data) \ "server_name").as[String]
        val user = (Json.parse(event.data) \ "user").as[String]
        (server_name, user)
      }
    }

    sourceFuture.map { source: Source[ServerSentEvent, NotUsed] =>
      source
        .via(parserFlow)
        .to(printSink).run()
    }
  }
}

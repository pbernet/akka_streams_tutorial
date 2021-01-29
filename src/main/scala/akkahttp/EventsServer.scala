package akkahttp

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.RouteResult._
import akkahttp.guardrail.definitions.Event
import akkahttp.guardrail.events.EventsResource.PostEventsResponse
import akkahttp.guardrail.events.{EventsClient, EventsHandler, EventsResource}
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalDate
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Server (route) and client are generated with sbt guardrail plugin,
  * https://github.com/twilio/sbt-guardrail
  *
  * Remarks:
  *  - Guardrail declarations are in /resources/events.yaml and in build.sbt
  *  - `sbt compile` kicks off generator plugin, gen. files are in folder /target/src_managed
  *  - Sometimes the target folder needs to be cleaned manually
  *  - JSON serialisation is done with circe
  *
  * No streams here
  *
  * Doc:
  * https://guardrail.dev/scala/akka-http
  *
  */
object EventsServer extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("EventsServer")
  implicit val executionContext = system.dispatcher

  val (address, port) = ("127.0.0.1", 8000)

  val routeFn: EventsHandler => Route = EventsResource.routes(_:EventsHandler)
  val route: Route = routeFn(new EventsHandler {
    // This is the implementation
    override def postEvents(respond: EventsResource.PostEventsResponse.type)(body: Vector[Event]): Future[PostEventsResponse] = {
      logger.info(s"Received events: ${body.size}")
      //throw new RuntimeException("Boom server error")
      Future(respond.OK)
    }
  })
  val bindingFuture = Http().newServerAt(address, port).bindFlow(route)

  bindingFuture.onComplete {
    case Success(b) =>
      logger.info("Server started, listening on: " + b.localAddress)
    case Failure(e) =>
      logger.info(s"Server could not bind to... Exception message: ${e.getMessage}")
      system.terminate()
  }

  def client() = {

    val retryingHttpClient = { nextClient: (HttpRequest => Future[HttpResponse]) =>
      req: HttpRequest => nextClient(req).flatMap(resp => if (resp.status.intValue >= 500) {
        logger.info("Retry...")
        nextClient(req)
      } else Future.successful(resp))
    }

    val singleRequestHttpClient = { (req: HttpRequest) => Http().singleRequest(req)}

    val client = EventsClient.httpClient(retryingHttpClient(singleRequestHttpClient), s"http://$address:$port")
    val event =  Event(Some("name"), Some(10), Some(LocalDate.now()))
    val events = List(event).toVector

    val response = client.postEvents(events, List.empty).value
    response.onComplete(each => logger.info(s"Client received response: $each") )
  }

  client()
}

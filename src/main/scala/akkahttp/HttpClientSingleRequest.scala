package akkahttp

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse}

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Http client for a single request from Doc:
  * https://doc.akka.io/docs/akka-http/current/client-side/request-level.html#request-level-client-side-api
  *
  */
object HttpClientSingleRequest extends App {
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = "https://akka.io"))
  responseFuture
    .onComplete {
      case Success(res) =>
        // Even if we don’t care about the response entity, we must consume it
        res.entity.discardBytes()
        println(s"Success: ${res.status}")
      case Failure(ex) => sys.error(s"Something wrong: ${ex.getMessage}")
    }
}
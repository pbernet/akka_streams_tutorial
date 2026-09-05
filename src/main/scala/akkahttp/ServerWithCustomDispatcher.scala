package akkahttp

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{HttpResponse, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives.{complete, extractRequest, path, withExecutionContext}
import org.apache.pekko.http.scaladsl.server.Route
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

/**
  * By default, route directives run on the default dispatcher.
  * The `withExecutionContext` directive alone doesn't force execution on the custom dispatcher
  * it only makes the custom execution context available.
  * Wrapping a task in a Future explicitly runs the task on the custom execution context.
  *
  * Full example for this answer:
  * https://stackoverflow.com/questions/79141989/executioncontext-issue-in-akka-http-server/79145603#79145603
  */
object ServerWithCustomDispatcher {
  def main(args: Array[String]): Unit = {
    bindingFuture.onComplete {
      case Success(b) =>
        println("Server started, listening on: http://" + b.localAddress)
      case Failure(e) =>
        println(s"Server could not bind to... Exception message: ${e.getMessage}")
        system.terminate()
    }
  }
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit lazy val system: ActorSystem = ActorSystem()

  implicit lazy val myExCon: ExecutionContextExecutor = system.dispatchers.lookup(
    "custom-dispatcher-fork-join"
  )

  val route: Route = {
    path("hello") {
      withExecutionContext(myExCon) {
        extractRequest { request =>
          // Move the operation inside a Future to ensure it runs on the custom dispatcher
          val result = Future {
            logger.info(s"Got request from client: ${request.getHeader("User-Agent")}")
            val msg = s"Execution context: $myExCon with thread: ${Thread.currentThread.getName}"
            logger.info(msg)
            HttpResponse(StatusCodes.OK, entity = s"$msg")
          }(using myExCon)
          complete(result)
        }
      }
    }
  }

  lazy val bindingFuture = Http().newServerAt("localhost", 9000).bindFlow(route)
}

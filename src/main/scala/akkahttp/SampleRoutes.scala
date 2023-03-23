package akkahttp

import actor.FaultyActor
import actor.FaultyActor.DoIt
import akka.actor.{ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes.InternalServerError
import akka.http.scaladsl.model.{ContentTypes, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.util.Timeout
import org.slf4j.{Logger, LoggerFactory}
import spray.json.DefaultJsonProtocol

import java.nio.file.Paths
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.sys.process.{Process, stringSeqToProcess}
import scala.util.{Failure, Success}

/**
  * Shows some (lesser known) directives from the rich feature set:
  * https://doc.akka.io/docs/akka-http/current/routing-dsl/directives/alphabetically.html
  *
  * Also shows exception handling according to:
  * https://doc.akka.io/docs/akka-http/current/routing-dsl/exception-handling.html?_ga=2.19174588.527792075.1647612374-1144924589.1645384786#exception-handling
  *
  * No streams here
  *
  */
object SampleRoutes extends App with DefaultJsonProtocol with SprayJsonSupport {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import spray.json._
  import system.dispatcher

  val faultyActor = system.actorOf(Props[FaultyActor](), "FaultyActor")

  case class FaultyActorResponse(totalAttempts: Int)

  implicit def fileInfoFormat: RootJsonFormat[FaultyActorResponse] = jsonFormat1(FaultyActorResponse)

  val rejectionHandler = RejectionHandler.newBuilder()
    .handle { case ValidationRejection(msg, _) => complete(StatusCodes.InternalServerError, msg) }
    .handleNotFound(complete(StatusCodes.NotFound, "Page not found"))
    .result()

  val exceptionHandler: ExceptionHandler = ExceptionHandler {
    case _: IllegalArgumentException =>
      complete(StatusCodes.BadRequest, "Illegal argument passed")
    case e: RuntimeException =>
      complete(HttpResponse(InternalServerError, entity = e.getMessage))
  }

  val getFromBrowsableDir: Route = {
    val dirToBrowse = System.getProperty("java.io.tmpdir")
    logger.info(s"Browse dir: $dirToBrowse")

    // pathPrefix allows loading dirs and files recursively
    pathPrefix("entries") {
      getFromBrowseableDirectory(dirToBrowse)
    }
  }

  val parseFormData: Route =
  // Set loglevel to "DEBUG" in application.conf for verbose akka-http log output
    logRequest("log post request") {
      post {
        path("post") {
          val minAge = 18
          formFields(Symbol("color"), Symbol("age").as[Int]) { (color, age) =>
            if (age > minAge) {
              logger.info(s"Age: $age is older than: $minAge")
              complete(s"The color is: $color and the age is: $age")
            } else {
              logger.error(s"Age: $age is younger than: $minAge")
              reject(ValidationRejection(s"Age: $age is too low"))
            }
          }
        }
      }
    }

  val getFromDocRoot: Route =
    get {
      val static = "src/main/resources"
      concat(
        pathSingleSlash {
          val appHtml = Paths.get(static, "SampleRoutes.html").toFile
          getFromFile(appHtml, ContentTypes.`text/html(UTF-8)`)
        },
        pathPrefix("static") {
          getFromDirectory(static)
        }
      )
    }

  val getFromFaultyActor =
    pathPrefix("faultyActor") {
      get {
        import akka.pattern.ask
        implicit val askTimeout: Timeout = Timeout(30.seconds)
        complete((faultyActor ? DoIt()).mapTo[FaultyActorResponse])
      }
    }

  // works with:
  // curl -X GET localhost:6002/acceptAll -H "Accept: application/json"
  // curl -X GET localhost:6002/acceptAll -H "Accept: text/csv"
  // curl -X GET localhost:6002/acceptAll -H "Accept: text/plain"
  // curl -X GET localhost:6002/acceptAll -H "Accept: text/xxx"
  val acceptAll: Route = get {
      path("acceptAll") { ctx: RequestContext =>
          // withAcceptAll: Remove/Ignore accept headers and always return application/json
         ctx.withAcceptAll.complete("""{ "foo": "bar" }""".parseJson)
      }
  }

  val handleErrors = handleRejections(rejectionHandler) & handleExceptions(exceptionHandler)

  val routes = {
    handleErrors {
      concat(getFromBrowsableDir, parseFormData, getFromDocRoot, getFromFaultyActor, acceptAll)
    }
  }

  val bindingFuture = Http().newServerAt("127.0.0.1", 6002).bind(routes)

  bindingFuture.onComplete {
    case Success(b) =>
      println("Server started, listening on: " + b.localAddress)
    case Failure(e) =>
      println(s"Server could not bind to... Exception message: ${e.getMessage}")
      system.terminate()
  }

  def browserClient() = {
    val os = System.getProperty("os.name").toLowerCase
    if (os == "mac os x") Process(s"open http://127.0.0.1:6002").!
    else if (os == "windows 10") Seq("cmd", "/c", s"start http://127.0.0.1:6002").!
  }

  browserClient()

  sys.addShutdownHook {
    println("About to shutdown...")
    val fut = bindingFuture.map(serverBinding => serverBinding.terminate(hardDeadline = 3.seconds))
    println("Waiting for connections to terminate...")
    val onceAllConnectionsTerminated = Await.result(fut, 10.seconds)
    println("Connections terminated")
    onceAllConnectionsTerminated.flatMap { _ => system.terminate()
    }
  }
}

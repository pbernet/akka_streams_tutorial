package akkahttp

import java.io.File
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ContentTypes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.sys.process.Process
import scala.util.{Failure, Success}

/**
  * Akka http playground
  * No streams here
  *
  *
  */
object SampleRoutes extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("SampleRoutes")
  implicit val executionContext = system.dispatcher


  def getFromBrowsableDir: Route = {
    val dirToBrowse = File.separator + "tmp"

    // pathPrefix allows loading dirs and files recursively
    pathPrefix("entries") {
      getFromBrowseableDirectory(dirToBrowse)
    }
  }

  def parseFormData: Route = path("post") {
    formFields(Symbol("color"), Symbol("age").as[Int]) { (color, age) =>
      complete(s"The color is '$color' and the age is $age")
    }
  }

  def getFromDocRoot: Route =
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
    )}

  def routes: Route = {
    getFromBrowsableDir ~ parseFormData ~ getFromDocRoot
  }

  val bindingFuture = Http().newServerAt("127.0.0.1", 8000).bindFlow(routes)

  bindingFuture.onComplete {
    case Success(b) =>
      println("Server started, listening on: " + b.localAddress)
    case Failure(e) =>
      println(s"Server could not bind to... Exception message: ${e.getMessage}")
      system.terminate()
  }

  def browserClient() = {
    val os = System.getProperty("os.name").toLowerCase
    if (os == "mac os x") Process(s"open http://localhost:8000").!
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

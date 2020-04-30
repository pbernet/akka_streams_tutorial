package akkahttp

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import org.slf4j.{Logger, LoggerFactory}

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
    pathPrefix("entries") { getFromBrowseableDirectory(dirToBrowse) }
  }

  def parseFormData: Route = path("post") {
    formFields('color, 'age.as[Int]) { (color, age) =>
      complete(s"The color is '$color' and the age is $age")
    }
  }

  def routes: Route = {
  getFromBrowsableDir ~ parseFormData
}

  val bindingFuture = Http().bindAndHandle(routes, "127.0.0.1", 8000)
  bindingFuture.onComplete {
    case Success(b) =>
      println("Server started, listening on: " + b.localAddress)
    case Failure(e) =>
      println(s"Server could not bind to... Exception message: ${e.getMessage}")
      system.terminate()
  }

  def browserClient() = {
    val os = System.getProperty("os.name").toLowerCase
    if (os == "mac os x") Process("open ./src/main/resources/SampleRoutes.html").!
  }

  browserClient()
}

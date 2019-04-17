package alpakka.env

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{MediaTypes, StatusCodes}
import akka.http.scaladsl.server.Directives.{logRequestResult, path, _}
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success}

/**
  * Dummy HTTP FileServer for local download simulation
  * Normal response: /download/[id]
  * Flaky response:  /downloadflaky/[id]
  */
object FileServer extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("FileServer")
  implicit val executionContext = system.dispatcher
  implicit val materializerServer = ActorMaterializer()

  val (address, port) = ("127.0.0.1", 6001)
  server(address, port)

  def server(address: String, port: Int): Unit = {
    val resourceFileName = "payload.zip"

    def routes: Route = logRequestResult("FileServer") {
      path("download" / Segment) { id =>
        logger.info(s"TRACE_ID: $id Server received download request.")
        get {
          //return always the same file independent from the ID
          getFromFile(new File(getClass.getResource(s"/$resourceFileName").toURI), MediaTypes.`application/zip`)
        }
      } ~ path("downloadflaky" / Segment) { id =>
        logger.info(s"TRACE_ID: $id Server received flaky download request")
        get {
          if (id.toInt % 10 == 0) { // 10, 20, 30
            complete(randomErrorHttpStatusCode)
          } else if (id.toInt % 5 == 0) { // 5, 15, 25
            //Causes TimeoutException on client if sleep time > 5 sec
            randomSleeper
            getFromFile(new File(getClass.getResource(s"/$resourceFileName").toURI), MediaTypes.`application/zip`)
          } else {
            getFromFile(new File(getClass.getResource(s"/$resourceFileName").toURI), MediaTypes.`application/zip`)
          }
        }
      }
    }

    val bindingFuture = Http().bindAndHandle(routes, address, port)
    bindingFuture.onComplete {
      case Success(b) =>
        logger.info("Server started, listening on: http:/" + b.localAddress + "/download/id")
      case Failure(e) =>
        logger.info(s"Server could not bind to $address:$port. Exception message: ${e.getMessage}")
        system.terminate()
    }
  }

  def randomSleeper = {
    val (start, end) = (1000, 10000)
    val rnd = new scala.util.Random
    val sleepTime = start + rnd.nextInt((end - start) + 1)
    logger.info(s" -> Sleep for $sleepTime milli seconds")
    Thread.sleep(sleepTime.toLong)
  }

  def randomErrorHttpStatusCode = {
    val statusCodes = Seq(StatusCodes.NotFound, StatusCodes.InternalServerError, StatusCodes.BadRequest, StatusCodes.ServiceUnavailable)
    val start = 0
    val end = statusCodes.size - 1
    val rnd = new scala.util.Random
    val finalRnd = start + rnd.nextInt((end - start) + 1)
    val statusCode = statusCodes(finalRnd)
    logger.info(s" -> Complete with HTTP status code: $statusCode")
    statusCodes(finalRnd)
  }
}
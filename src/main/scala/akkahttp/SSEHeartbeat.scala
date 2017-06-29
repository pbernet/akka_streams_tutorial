package akkahttp

import java.time.LocalTime
import java.time.format.DateTimeFormatter

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}
import akka.http.scaladsl.client.RequestBuilding.Get

object SSEHeartbeat {
  implicit val system = ActorSystem("my-system")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  def main(args: Array[String]) {
    val (address, port) = ("127.0.0.1", 6000)
    server(address, port)
    client(address, port)
  }

  private def server(address: String, port: Int) = {

    val route = {
      import akka.http.scaladsl.marshalling.sse.EventStreamMarshalling._
      import akka.http.scaladsl.server.Directives._ // That does the trick!

     def timeToServerSentEvent(time: LocalTime) = ServerSentEvent(DateTimeFormatter.ISO_LOCAL_TIME.format(time))

      def events =
        path("events") {
          get {
            complete {
              Source
                .tick(2.seconds, 2.seconds, NotUsed)
                .map(_ => LocalTime.now())
                .map(timeToServerSentEvent)
                .keepAlive(1.second, () => ServerSentEvent.heartbeat)
            }
          }
        }
      events
    }

    val bindingFuture = Http().bindAndHandle(route, address, port)
    bindingFuture.onComplete {
      case Success(b) =>
        println("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        println(s"Server could not bind to $address:$port. Exception message: ${e.getMessage}")
        system.terminate()
    }
  }

  private def client(address: String, port: Int) = {

    import akka.http.scaladsl.unmarshalling.sse.EventStreamUnmarshalling._

    Http()
      .singleRequest(Get(s"http://$address:$port/events"))
      .flatMap(Unmarshal(_).to[Source[ServerSentEvent, NotUsed]])
      .foreach(_.runForeach(println))
  }
}

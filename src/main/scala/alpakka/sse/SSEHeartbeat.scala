package alpakka.sse

import java.time.LocalTime
import java.time.format.DateTimeFormatter

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream._
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
  * Basic heartbeat example, enhanced with an additional backoffClient which is recovering
  * thanks to the new RestartSource feature in akka 2.5.4 - see:
  * http://doc.akka.io/docs/akka/current/scala/stream/stream-error.html#delayed-restarts-with-a-backoff-stage
  *
  * An even more resilient sse server->client implementation is here:
  * http://developer.lightbend.com/docs/alpakka/current/sse.html
  * see EventSourceSpec in this repo for a working example
  */
object SSEHeartbeat {
  implicit val system = ActorSystem("SSEHeartbeat")
  implicit val executionContext = system.dispatcher
  implicit val materializerServer = ActorMaterializer()

  def main(args: Array[String]) {
    val (address, port) = ("127.0.0.1", 6000)
    server(address, port)
    simpleClient(address, port) //is not recovering after RuntimeException on server
    backoffClient(address, port) //is recovering after RuntimeException on server
  }

  private def server(address: String, port: Int) = {

    val route = {
      import akka.http.scaladsl.marshalling.sse.EventStreamMarshalling._
      import akka.http.scaladsl.server.Directives._ // That does the trick!

      def timeToServerSentEvent(time: LocalTime) = ServerSentEvent(DateTimeFormatter.ISO_LOCAL_TIME.format(time))

      def events =
        path("events" / Segment ){ clientName =>
          println(s"Server received request from $clientName")
          get {
            complete {
              Source
                .tick(2.seconds, 2.seconds, NotUsed)
                .map(_ => {
                  val time = LocalTime.now()
                  if (time.getSecond > 50) {println(s"Server RuntimeException at: $time"); throw new RuntimeException("Boom!")}
                  println(s"Send to client: $time")
                  time
                })
                .map(timeToServerSentEvent)
                .keepAlive(1.second, () => ServerSentEvent.heartbeat) //works as well: intersperse(ServerSentEvent.heartbeat)
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

  private def simpleClient(address: String, port: Int) = {

    import akka.http.scaladsl.unmarshalling.sse.EventStreamUnmarshalling._

    Http()
      .singleRequest(HttpRequest(
        uri = s"http://$address:$port/events/simpleClient"
      ))
      .flatMap(Unmarshal(_).to[Source[ServerSentEvent, NotUsed]])
      .foreach(_.runForeach(event => println(s"simpleClient got event: $event")))
  }

  private def backoffClient(address: String, port: Int) = {

    import akka.http.scaladsl.unmarshalling.sse.EventStreamUnmarshalling._

    val restartSource = RestartSource.withBackoff(
      minBackoff = 3.seconds,
      maxBackoff = 30.seconds,
      randomFactor = 0.2 // adds 20% "noise" to vary the intervals slightly
    ) { () =>
      Source.fromFutureSource {
        Http()
          .singleRequest(HttpRequest(
            uri = s"http://$address:$port/events/backoffClient"
          ))
          .flatMap(Unmarshal(_).to[Source[ServerSentEvent, NotUsed]])
      }
    }

    val (killSwitch: UniqueKillSwitch, done) = restartSource
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.foreach(event => println(s"backoffClient got event: $event")))(Keep.both)
      .run()

    //See PrintMoreNumbers for correctly stopping the stream
    done.map(_ => {println("Reached shutdown..."); killSwitch.shutdown()} )
  }
}

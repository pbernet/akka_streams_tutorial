package alpakka.sse

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.sse.ServerSentEvent
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.stream._
import org.apache.pekko.stream.scaladsl.{Keep, RestartSource, Sink, Source}

import java.time.LocalTime
import java.time.format.DateTimeFormatter
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
  * Basic heartbeat example, enhanced with an additional backoffClient which is recovering
  * after RuntimeException on server, see Doc RestartSource:
  * https://doc.akka.io/docs/akka/current/stream/stream-error.html?language=scala#delayed-restarts-with-a-backoff-operator
  *
  */
object SSEHeartbeat extends App {
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val (address, port) = ("127.0.0.1", 6000)
  server(address, port)
  simpleClient(address, port)  // is not recovering after RuntimeException on server
  backoffClient(address, port) // is recovering after RuntimeException on server

  private def server(address: String, port: Int) = {

    val route = {
      import org.apache.pekko.http.scaladsl.marshalling.sse.EventStreamMarshalling._
      import org.apache.pekko.http.scaladsl.server.Directives._

      def timeToServerSentEvent(time: LocalTime) = ServerSentEvent(DateTimeFormatter.ISO_LOCAL_TIME.format(time))

      def events =
        path("events" / Segment) { clientName =>
          println(s"Server received request from $clientName")
          get {
            complete {
              Source
                .tick(2.seconds, 2.seconds, NotUsed)
                .map(_ => {
                  val time = LocalTime.now()
                  if (time.getSecond > 50) {
                    println(s"Server RuntimeException at: $time");
                    throw new RuntimeException("BOOM - server RuntimeException")
                  }
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

    val bindingFuture = Http().newServerAt(address, port).bindFlow(route)
    bindingFuture.onComplete {
      case Success(b) =>
        println("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        println(s"Server could not bind to $address:$port. Exception message: ${e.getMessage}")
        system.terminate()
    }
  }

  private def simpleClient(address: String, port: Int) = {

    import org.apache.pekko.http.scaladsl.unmarshalling.sse.EventStreamUnmarshalling._

    Http()
      .singleRequest(HttpRequest(
        uri = s"http://$address:$port/events/simpleClient"
      ))
      .flatMap(Unmarshal(_).to[Source[ServerSentEvent, NotUsed]])
      .foreach(_.runForeach(event => println(s"simpleClient got event: $event")))
  }

  private def backoffClient(address: String, port: Int) = {

    import org.apache.pekko.http.scaladsl.unmarshalling.sse.EventStreamUnmarshalling._

    val restartSettings = RestartSettings(1.second, 10.seconds, 0.2).withMaxRestarts(10, 1.minute)
    val restartSource = RestartSource.withBackoff(restartSettings) { () =>
      Source.futureSource {
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
    done.map(_ => {
      println("Reached shutdown...");
      killSwitch.shutdown()
    })
  }
}

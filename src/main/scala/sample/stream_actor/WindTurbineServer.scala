package sample.stream_actor

import akka.Done
import akka.actor.{ActorSystem, Props}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.stream.scaladsl.{Flow, Source}
import akka.util.Timeout
import play.api.libs.json._
import sample.stream_actor.Total.Increment

import scala.collection.immutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}


/**
  * Sample Implementation of:
  * http://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-i
  *
  * WindTurbineServer receives [[Measurements]] via Websockets from n clients
  * Clients are started with [[SimulateWindTurbines]]
  *
  */
object WindTurbineServer {
  protected implicit val system = ActorSystem("WindTurbineServer")
  implicit def executor: ExecutionContext = system.dispatcher
  protected val log = Logging(system.eventStream, "WindTurbineServer-main")

  object Messages  {

    def parse(messages: immutable.Seq[String]): Seq[MeasurementsContainer] = messages.map { message =>
      implicit val measurementsFormat = Json.format[Measurements]
      implicit val windTurbineDataFormat = Json.format[MeasurementsContainer]
      Json.parse(message).as[MeasurementsContainer]
    }

    def ack(aString: String) = TextMessage(Source.single("Ack from server: " + aString))
  }


  def main(args: Array[String]): Unit = {
    val total = system.actorOf(Props[Total], "total")

    def average[T]( ts: Iterable[T] )( implicit num: Numeric[T] ) = {
      val avg = num.toDouble( ts.sum ) / ts.size
      (math floor avg * 100) / 100
    }

    //compute intermediate sums (= number of measurements) and send them to the Total actor, at least every second
    val measurementsWebSocketFlow: Flow[Message, Message, Any] =
      Flow[Message]
        .collect {
          case TextMessage.Strict(text) =>
            Future.successful(text)
          case TextMessage.Streamed(textStream) =>
            textStream.runFold("")(_ + _)
              .flatMap(Future.successful)
        }
        .mapAsync(1)(x => x) //identity function
        .groupedWithin(100, 1.second)
        .map(messages => (messages.last, Messages.parse(messages)))
        .map { elem => println(s"After parsing size: ${elem._2.size}"); elem}
        .mapAsync(1) {
          case (lastMessage: String, measurements: Seq[MeasurementsContainer]) =>
            import akka.pattern.ask
            implicit val askTimeout = Timeout(30.seconds)

            // Optional: generate server errors at 1/6 of the time
            // Clients receive:
            // akka.http.scaladsl.model.ws.PeerClosedConnectionException: Peer closed connection with code 1011 'internal error'
            // and are able to recover due to the RestartSource

            //val time = LocalTime.now()
            //if (time.getSecond > 50) {println(s"Server RuntimeException at: $time"); throw new RuntimeException("Boom!")}

            //only send a single message at a time to the Total actor, backpressure otherwise
            val windSpeeds = measurements.map(each => each.measurements.wind_speed)
            (total ? Increment(measurements.size, average(windSpeeds), measurements.head.id))
              .mapTo[Done]
              .map(_ => lastMessage)
        }
        .map(Messages.ack) //ack the last message only


    val route =
      path("measurements" / JavaUUID ) { id =>
        get {
          println(s"Receiving WindTurbineData form: $id")
          handleWebSocketMessages(measurementsWebSocketFlow)
        }
      }

    val httpInterface = "127.0.0.1"
    val httpPort = 8080

    log.info(s"About ot bind to: $httpInterface and: $httpPort")
    val bindingFuture: Future[ServerBinding] = Http().newServerAt(httpInterface, httpPort).bindFlow(route)

    bindingFuture.map { serverBinding =>
      log.info(s"Bound to: ${serverBinding.localAddress} ")
    }.onComplete {
      case Success(_) => log.info("WindTurbineServer started successfully")
      case Failure(ex) =>
        log.error(ex, "Failed to bind to {}:{}!", httpInterface, httpPort)
        Http().shutdownAllConnectionPools()
        system.terminate()
    }

    scala.sys.addShutdownHook {
      log.info("Terminating...")
      Http().shutdownAllConnectionPools()
      //actor system termination in 2.6.x is now implicit, see:
      //https://github.com/akka/akka/issues/28310
      log.info("Terminated... Bye")
    }
  }
}

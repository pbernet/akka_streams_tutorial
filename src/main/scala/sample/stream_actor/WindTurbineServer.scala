package sample.stream_actor

import akka.Done
import akka.actor.{ActorSystem, Props}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.util.Timeout
import sample.stream_actor.Total.Increment

import scala.collection.immutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}


/**
  * Bootstrap a server
  * http://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-i
  */
object WindTurbineServer {

  protected implicit val system = ActorSystem("WindTurbineServer")

  implicit def executor: ExecutionContext = system.dispatcher
  protected val log = Logging(system.eventStream, "WindTurbineServer-main")
  protected implicit val materializer: ActorMaterializer = ActorMaterializer()

  object Messages  {
    //TODO parse from TextMessage (= JSON) to case class Measurements, although not needed for Total
    def parse(messages: immutable.Seq[String]): Seq[Measurements] = messages.map(message => Measurements(1,1,1))
    def ack(aString: String) = TextMessage(aString)
  }

  case class Measurements(power: Long, rotor_speed: Long, wind_speed: Long)


  def main(args: Array[String]): Unit = {
    val total = system.actorOf(Props[Total], "total")

    //computes intermediate sums (= Number of measurements) and sends them to the Total actor, at least every second
    val measurementsWebSocket: Flow[Message, Message, Any] =
      Flow[Message]
        .collect {
          case TextMessage.Strict(text) =>
            Future.successful(text)
          case TextMessage.Streamed(textStream) =>
            textStream.runFold("")(_ + _)
              .flatMap(Future.successful)
        }
        .mapAsync(1)(x => x) //identity function
        //.map { elem => println(s"Before grouping $elem"); elem }
        .groupedWithin(1000, 1.second)
        .map(messages => (messages.last, Messages.parse(messages)))
        .map { elem => println(s"After parsing size: ${elem._2.size}"); elem}
        .mapAsync(1) {
          case (lastMessage: String, measurements: Seq[WindTurbineServer.Measurements]) =>
            import akka.pattern.ask
            implicit val askTimeout = Timeout(30.seconds)
            //only send a single message at a time to the Total actor, backpressuring otherwise
            (total ? Increment(measurements.size))
              .mapTo[Done]
              .map(_ => lastMessage)
        }
        .map(Messages.ack)


    val route =
      //PathMatchers.uuid / JavaUUID
      path("measurements" ) {
        get {
          println(s"Recieved request for path")
          handleWebSocketMessages(measurementsWebSocket)
          //complete {StatusCodes.OK}
        }
      }


    val httpInterface = "127.0.0.1"
    val httpPort = 8080

    log.info(s"About ot bind to: $httpInterface and: $httpPort")
    val bindingFuture: Future[ServerBinding] = Http().bindAndHandle(route, httpInterface, httpPort)

    bindingFuture.map { serverBinding =>
      log.info(s"Bound to ${serverBinding.localAddress} ")
    }.onFailure {
      case ex: Exception =>
        log.error(ex, "Failed to bind to {}:{}!", httpInterface, httpPort)
        Http().shutdownAllConnectionPools()
        system.terminate()
    }

    scala.sys.addShutdownHook {
      log.info("Terminating...")
      Http().shutdownAllConnectionPools()
      system.terminate()
      Await.result(system.whenTerminated, 30.seconds)
      log.info("Terminated... Bye")
    }
  }



}

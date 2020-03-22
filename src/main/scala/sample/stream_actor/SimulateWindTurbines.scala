package sample.stream_actor

import akka.actor.ActorSystem
import akka.pattern.{BackoffOpts, BackoffSupervisor}
import akka.stream.ThrottleMode
import akka.stream.scaladsl.{Sink, Source}

import scala.concurrent.duration._

/**
  * Sample Implementation of:
  * http://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-ii
  *
  * Starts n [[WindTurbineSimulator]], which generate [[WindTurbineData]]
  * Uses a [[BackoffSupervisor]] as level of indirection
  *
  * The server is started with [[WindTurbineServer]]
  */
object SimulateWindTurbines extends App {
  implicit val system = ActorSystem()

  val endpoint = "ws://127.0.0.1:8080"
  val numberOfTurbines = 5
  Source(1 to numberOfTurbines)
    .throttle(
      elements = 100, //number of elements to be taken from bucket
      per = 1.second,
      maximumBurst = 100, //capacity of bucket
      mode = ThrottleMode.shaping
    )
    .map { _ =>
      val id = java.util.UUID.randomUUID.toString

      val supervisor = BackoffSupervisor.props(
        BackoffOpts.onFailure(
          WindTurbineSimulator.props(id, endpoint),
          childName = id,
          minBackoff = 1.second,
          maxBackoff = 30.seconds,
          randomFactor = 0.2
        ))

      system.actorOf(supervisor, name = s"$id-backoff-supervisor")
    }
    .runWith(Sink.ignore)
}

package sample.stream_actor

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ThrottleMode}
import akka.stream.scaladsl.{Sink, Source}
import sample.WindTurbineSimulator

import scala.concurrent.duration._

/**
  Sample Implementation of:
  http://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-ii
  **/
object SimulateWindTurbines extends App {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  val endpoint = "ws://127.0.0.1:8080"
  val numberOfTurbines = 1000
  Source(1 to numberOfTurbines)
    .throttle(
      elements = 100, //number of elements to be taken from bucket
      per = 1.second,
      maximumBurst = 100,  //capacity of the bucket
      mode = ThrottleMode.shaping
    )
    .map { _ =>
      val id = java.util.UUID.randomUUID.toString
      system.actorOf(WindTurbineSimulator.props(id, endpoint), id)
    }
    .runWith(Sink.ignore)
}

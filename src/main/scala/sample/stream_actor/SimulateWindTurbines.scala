package sample.stream_actor

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ThrottleMode}
import akka.stream.scaladsl.{Sink, Source}
import sample.WindTurbineSimulator

import scala.concurrent.duration._

/**
  Sample Implementation
  http://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-ii
  **/
object SimulateWindTurbines extends App {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  val endpoint = "http://localhost:8080"

  Source(1 to 1000)
    .throttle(
      elements = 100,
      per = 1.second,
      maximumBurst = 100,
      mode = ThrottleMode.shaping
    )
    .map { _ =>
      val id = java.util.UUID.randomUUID.toString
      system.actorOf(WindTurbineSimulator.props(id, endpoint), id)
    }
    .runWith(Sink.ignore)
}

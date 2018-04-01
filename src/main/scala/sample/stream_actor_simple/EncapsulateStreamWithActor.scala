package sample.stream_actor_simple

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer

import scala.concurrent.duration._

/**
  * Inspired by:
  * http://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-ii
  *
  */
object EncapsulateStreamWithActor extends App {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher
  val actorRef = system.actorOf(Props(classOf[PrintMoreNumbers], materializer))
  system.scheduler.scheduleOnce(10.seconds) {
    actorRef ! "stop"
  }
}
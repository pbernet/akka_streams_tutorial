package sample.stream_actor_simple

import org.apache.pekko.actor.{ActorSystem, Props}

import scala.concurrent.duration._

/**
  * Inspired by:
  * http://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-ii
  *
  */
object EncapsulateStreamWithActor extends App {
  implicit val system: ActorSystem = ActorSystem()
  implicit val executionContext = system.dispatcher

  val actorRef = system.actorOf(Props(classOf[PrintMoreNumbers]))
  system.scheduler.scheduleOnce(10.seconds) {
    actorRef ! "stop"
  }
}
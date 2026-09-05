package sample.stream_actor_simple

import org.apache.pekko.actor.{ActorSystem, Props}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.*

/**
  * Inspired by:
  * http://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-ii
  *
  */
object EncapsulateStreamWithActor {
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    val actorRef = system.actorOf(Props(classOf[PrintMoreNumbers]))
    system.scheduler.scheduleOnce(10.seconds) {
      actorRef ! "stop"
    }
  }
}

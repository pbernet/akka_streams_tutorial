package sample.stream_actor_simple

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer
import scala.concurrent.duration._

object LessTrivialExample extends App {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher
  val actorRef = system.actorOf(Props(classOf[PrintMoreNumbers], materializer))
  system.scheduler.scheduleOnce(10 seconds) {
    actorRef ! "stop"
  }
}
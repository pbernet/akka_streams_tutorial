package kafka

import akka.Done
import akka.actor.Actor
import kafka.TotalFake.Increment

/**
  * Keeps the state of the count
  *
  */
object TotalFake {
  case class Increment(value: Int, id: String)
}

class TotalFake extends Actor {
  var total: Int = 0

  override def receive: Receive = {
    case Increment(value, id) =>
      println(s"The new current total for fakenews count is: $value. Plus ${value - total} from last count.")
      total = value
      sender ! Done
  }
}
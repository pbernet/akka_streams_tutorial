package sample.stream_actor

import akka.Done
import akka.actor.Actor
import sample.stream_actor.Total.Increment
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

object Total {
  case class Increment(value: Long, id: String)
}

class Total extends Actor {
  var total: Long = 0

  override def receive: Receive = {
    case Increment(value, id) =>
      //println(s"Recieved increment from id: $id")
      total = total + value

      val date = new Date()
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      df.setTimeZone(TimeZone.getTimeZone("Europe/Zurich"))

      println(s"${df.format(date) } - Current total of all measurements: $total")
      sender ! Done
  }
}
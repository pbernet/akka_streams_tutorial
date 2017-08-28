package sample.stream_actor

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import akka.Done
import akka.actor.Actor
import sample.stream_actor.Total.Increment

object Total {
  case class Increment(value: Long, avg: Double, id: String)
}

class Total extends Actor {
  var total: Long = 0

  override def receive: Receive = {
    case Increment(value, avg, id) =>
      println(s"Received $value new measurements from turbine with id: $id -  Avg wind speed is: $avg")
      total = total + value

      val date = new Date()
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      df.setTimeZone(TimeZone.getTimeZone("Europe/Zurich"))

      println(s"${df.format(date) } - Current total of all measurements: $total")
      sender ! Done
  }
}
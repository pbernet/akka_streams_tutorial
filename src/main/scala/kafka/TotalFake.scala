package kafka

import akka.Done
import akka.actor.Actor
import kafka.TotalFake.{IncrementMessage, IncrementWord}

/**
  * Keep the state of
  *  - WORD count
  *  - MESSAGE count
  *
  * Since a message can contain several "fakeNews" keywords the number of words is higher
  */
object TotalFake {
  case class IncrementWord(value: Int, id: String)
  case class IncrementMessage(value: Int, id: String)
}

class TotalFake extends Actor {
  var totalWords: Int = 0
  var totalNews: Int = 0

  override def receive: Receive = {
    case IncrementWord(value, id) =>
      println(s"$id - WORD count: $value (+ ${value - totalWords})")
      totalWords = value
      sender ! Done

    case IncrementMessage(value, id) =>
      totalNews += value
      println(s"$id - MESSAGE count: $totalNews (+ ${value})")
      sender ! Done
  }
}
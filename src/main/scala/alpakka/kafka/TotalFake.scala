package alpakka.kafka

import akka.Done
import akka.actor.Actor
import alpakka.kafka.TotalFake.{IncrementMessage, IncrementWord}

/**
  * Keep the state of:
  *  - WORD count for keyword "fakeNews"
  *  - MESSAGE count for messages which contain the keyword "fakeNews"
  *
  * Note that a message can contain several "fakeNews" keywords
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
      println(s"$id - WORD count fakeNews: $value (+ ${value - totalWords})")
      totalWords = value
      sender() ! Done

    case IncrementMessage(value, id) =>
      totalNews += value
      println(s"$id - MESSAGES count: $totalNews (+ $value)")
      sender() ! Done
  }
}
package alpakka.kafka

import alpakka.kafka.TotalFake.{IncrementMessage, IncrementWord}
import org.apache.pekko.Done
import org.apache.pekko.actor.Actor
import org.apache.pekko.kafka.ConsumerMessage.CommittableMessage

/**
  * Keep the state of:
  *  - WORD count for keyword "fakeNews"
  *  - MESSAGE count for messages which contain the keyword "fakeNews"
  *
  * Note that a message can contain several "fakeNews" keywords
  */
object TotalFake {
  case class IncrementWord(msg: CommittableMessage[String, java.lang.Long], id: String)
  case class IncrementMessage(msg: CommittableMessage[String, java.lang.Long], id: String)
}

class TotalFake extends Actor {
  var totalWords: Int = 0
  var totalNews: Int = 0

  override def receive: Receive = {
    case IncrementWord(msg, id) =>
      val newValue = msg.record.value().toInt

      if (msg.record.key().equalsIgnoreCase("fakeNews")) {
        println(s"$id - WORD count fakeNews: $newValue (+ ${newValue - totalWords})")
        totalWords = newValue
      }
      sender() ! Done

    case IncrementMessage(msg, id) =>
      val newValue = msg.record.value.toInt

      totalNews += newValue
      println(s"$id - MESSAGE count: $totalNews (+ $newValue)")
      sender() ! Done
  }
}
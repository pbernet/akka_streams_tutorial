package kafka

import akka.Done
import akka.actor.{ActorSystem, Props}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import kafka.TotalFake.{IncrementMessage, IncrementWord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Consumers W.1 and W.2 consume half of the partitions each within the wordcount consumer group
  * Consumer M is a single consumer for all the partitions in the messagecount consumer group
  *
  * Use the offset storage in Kafka:
  * https://doc.akka.io/docs/akka-stream-kafka/current/consumer.html#offset-storage-in-kafka-committing
  *
  */
object WordCountConsumer extends App {
  implicit val system = ActorSystem()
  implicit val ec = system.dispatcher

  val total = system.actorOf(Props[TotalFake], "totalFake")

  def createConsumerSettings(group: String): ConsumerSettings[String, java.lang.Long] = {
    ConsumerSettings(system, new StringDeserializer , new LongDeserializer)
      .withBootstrapServers("localhost:9092")
      .withGroupId(group)
      //Define consumer behavior upon starting to read a partition for which it does not have a committed offset or if the committed offset it has is invalid
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  }

  def createAndRunConsumerWordCount(id: String) = {
    Consumer.committableSource(createConsumerSettings("wordcount consumer group"), Subscriptions.topics("wordcount-output"))
      .mapAsync(1) { msg =>
        println(s"$id - Offset: ${msg.record.offset()} - Partition: ${msg.record.partition()} Consume msg with key: ${msg.record.key()} and value: ${msg.record.value()}")
        if (msg.record.key() == "fakenews") {
          import akka.pattern.ask
          implicit val askTimeout: Timeout = Timeout(30.seconds)
          (total ? IncrementWord(msg.record.value.toInt, id)).mapTo[Done]
        }
        Future(msg)
      }
      .mapAsync(1) { msg =>
        msg.committableOffset.commitScaladsl() //commit after processing gives an “at-least once delivery”
      }
      .runWith(Sink.ignore)
  }

  def createAndRunConsumerMessageCount(id: String) = {
    Consumer.committableSource(createConsumerSettings("messagecount consumer group"), Subscriptions.topics("messagecount-output"))
      .mapAsync(1) { msg =>
        //println(s"$id - Offset: ${msg.record.offset()} - Partition: ${msg.record.partition()} Consume msg with key: ${msg.record.key()} and value: ${msg.record.value()}")
        import akka.pattern.ask
        implicit val askTimeout: Timeout = Timeout(30.seconds)
        (total ? IncrementMessage(msg.record.value.toInt, id)).mapTo[Done]
        Future(msg)
      }
      .mapAsync(1) { msg =>
        msg.committableOffset.commitScaladsl() //commit after processing for “at-least once delivery”
      }
      .runWith(Sink.ignore)
  }

  createAndRunConsumerWordCount("W.1")
  createAndRunConsumerWordCount("W.2")
  createAndRunConsumerMessageCount("M")


  sys.addShutdownHook{
    println("Got control-c cmd from shell, about to shutdown...")
    system.terminate()
  }
}

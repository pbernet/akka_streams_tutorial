package alpakka.tcp_to_websockets.hl7mllp

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Keep
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.Future

/**
  * Consumer M is a single consumer for all the partitions in the hl7-input consumer group
  *
  * Use the offset storage in Kafka:
  * https://doc.akka.io/docs/akka-stream-kafka/current/consumer.html#offset-storage-in-kafka-committing
  *
  * TODO Add transactional and restart behaviour
  *
  */
object Kafka2Websocket extends App {
  implicit val system = ActorSystem("Kafka2Websocket")
  implicit val ec = system.dispatcher

  val committerSettings = CommitterSettings(system)
  val bootstrapServers = "localhost:9092"

  private def createConsumerSettings(group: String): ConsumerSettings[String, String] = {
    ConsumerSettings(system, new StringDeserializer , new StringDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withGroupId(group)
      //Define consumer behavior upon starting to read a partition for which it does not have a committed offset or if the committed offset it has is invalid
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  }

  private def createAndRunConsumerMessageCount(id: String) = {
    Consumer.committableSource(createConsumerSettings("hl7-input consumer group"), Subscriptions.topics("hl7-input"))
      .mapAsync(1) { msg =>
        println(s"$id - Offset: ${msg.record.offset()} - Partition: ${msg.record.partition()} Consume msg with key: ${msg.record.key()} and value: ${printable(msg.record.value())}")
       //TODO Push to websocket client via flow
        Future(msg).map(_ => msg.committableOffset)
      }
      .toMat(Committer.sink(committerSettings))(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()
  }

  // The HAPI parser needs /r as segment terminator, but this is not printable
  private def printable(message: String): String = {
    message.replace("\r", "\n")
  }

  sys.addShutdownHook{
    println("Got control-c cmd from shell, about to shutdown...")
  }
  createAndRunConsumerMessageCount("M")
}
package kafka

import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerMessage, ProducerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.concurrent.duration._

/**
  * Skeleton
  *
  * The word count should probably happen like described here:
  * http://doc.akka.io/docs/akka/2.5.4/scala/stream/stream-cookbook.html#implementing-reduce-by-key
  * and here:
  * http://doc.akka.io/docs/akka-stream-kafka/current/consumer.html#connecting-producer-and-consumer
  *
  *
  */
object WordCountReactiveKafka extends App {
  implicit val system = ActorSystem()
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()


  val bootstrapServers = "localhost:9092"
  //TODO Change to LongSerializer when count is working
  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)


  def createConsumerSettings(group: String): ConsumerSettings[String, String] = {
    ConsumerSettings(system, new StringDeserializer , new StringDeserializer)
      .withBootstrapServers("localhost:9092")
      .withGroupId(group)
      //behavior of the consumer when it starts reading a partition for which it doesn’t have a committed offset or if the committed offset it has is invalid
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withWakeupTimeout(10.seconds)
      .withMaxWakeups(10)
  }


  Consumer.committableSource(createConsumerSettings("wordcount consumer group"), Subscriptions.topics("wordcount-input"))
    //TODO Add the word count - Where is the commit handled? Should it be at the beginning?
    //split by whitespace "\\W+" : .mapConcat{msg => msg.record.value().split("\\W+").toList

    //filter "truth" and ""

    //groupBy
    //count

    .map { msg =>
      println(s"wordcount-input -> wordcount-output: $msg")
      ProducerMessage.Message(new ProducerRecord[String, String](
        "wordcount-outputtest",
        msg.record.value
      ), msg.committableOffset)
    }
    .runWith(Producer.commitableSink(producerSettings))  //commitableSink has “at-least once delivery”
}

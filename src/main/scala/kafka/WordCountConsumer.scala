package kafka

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Consumer, which uses the offset storage in Kafka:
  * http://doc.akka.io/docs/akka-stream-kafka/current/consumer.html#offset-storage-in-kafka
  *
  */
object WordCountConsumer extends App {
  implicit val system = ActorSystem()
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  def createConsumerSettings(group: String): ConsumerSettings[String, String] = {
    ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers("localhost:9092")
      .withGroupId(group)
      //behavior of the consumer when it starts reading a partition for which it doesn’t have a committed offset or if the committed offset it has is invalid
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withWakeupTimeout(10 seconds)
      .withMaxWakeups(10)
  }

  val done =
    Consumer.committableSource(createConsumerSettings("wordcount consumer group"), Subscriptions.topics("wordcount-output"))
      .mapAsync(1) { msg =>
        println(s"Offset: ${msg.record.offset()} Consume msg with key: ${msg.record.key()} and value: ${msg.record.value()}")
        Future(msg)
      }
      .mapAsync(1) { msg =>
        msg.committableOffset.commitScaladsl()
      }
      .runWith(Sink.ignore)

  done.onComplete(_ => {
    println("Finished processing, about to shutdown...")
    system.terminate()
  })
  sys.addShutdownHook{
    println("Got shutdown cmd from shell, about to shutdown...")
    system.terminate()
  }
}

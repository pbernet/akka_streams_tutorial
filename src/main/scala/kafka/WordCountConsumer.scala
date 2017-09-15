package kafka

import akka.Done
import akka.actor.{ActorSystem, Props}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import kafka.TotalFake.Increment
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Start two consumers A and B within the same group, which use the offset storage in Kafka:
  * http://doc.akka.io/docs/akka-stream-kafka/current/consumer.html#offset-storage-in-kafka
  *
  */
object WordCountConsumer extends App {
  implicit val system = ActorSystem()
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  val total = system.actorOf(Props[TotalFake], "totalFake")

  def createConsumerSettings(group: String): ConsumerSettings[String, java.lang.Long] = {
    ConsumerSettings(system, new StringDeserializer , new LongDeserializer)
      .withBootstrapServers("localhost:9092")
      .withGroupId(group)
      //behavior of the consumer when it starts reading a partition for which it doesn’t have a committed offset or if the committed offset it has is invalid
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withWakeupTimeout(10.seconds)
      .withMaxWakeups(10)
  }

  def createAndRunConsumer(id: String) = {
    Consumer.committableSource(createConsumerSettings("wordcount consumer group"), Subscriptions.topics("wordcount-output"))
      .mapAsync(1) { msg =>
        println(s"$id - Offset: ${msg.record.offset()} - Partition: ${msg.record.partition()} Consume msg with key: ${msg.record.key()} and value: ${msg.record.value()}")
        if (msg.record.key() == "fakenews") {
          import akka.pattern.ask
          implicit val askTimeout = Timeout(30.seconds)
          (total ? Increment(msg.record.value.toInt, id)).mapTo[Done]
        }
        Future(msg)
      }
      .mapAsync(1) { msg =>
        msg.committableOffset.commitScaladsl() //commit after processing gives an “at-least once delivery”
      }
      .runWith(Sink.ignore)
  }

  createAndRunConsumer("A")
  createAndRunConsumer("B") //seems to work, maybe not the best strategy to run a consumer group

  sys.addShutdownHook{
    println("Got control-c cmd from shell, about to shutdown...")
    system.terminate()
  }
}

package kafka

import java.lang

import akka.Done
import akka.actor.{ActorSystem, Props}
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerMessage, ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import kafka.TotalFake.IncrementWord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}

import scala.concurrent.duration._

/**
  * Source per partition:
  * http://doc.akka.io/docs/akka-stream-kafka/current/consumer.html#source-per-partition
  *
  * This consumer consumes WordCounts only
  * TODO Try to understand the documented benefits:
  *  * Supports tracking the automatic partition assignment from Kafka.
  *  * When topic-partition is assigned to a consumer this source will emit tuple with assigned topic-partition and a corresponding source.
  *  * When topic-partition is revoked then corresponding source completes.
  * compared to WordCountConsumer
  */
object WordCountConsumerPartitionedSource extends App {
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

    //Backpressure per partition with batch commit
    Consumer.committablePartitionedSource(createConsumerSettings("wordcount consumer group"), Subscriptions.topics("wordcount-output"))
      .flatMapMerge(2, _._2)
      .batch(max = 20, first => CommittableOffsetBatch.empty.updated(first.committableOffset)) { (batch: CommittableOffsetBatch, msg: ConsumerMessage.CommittableMessage[String, lang.Long]) =>
        println(s"$id - Offset: ${msg.record.offset()} - Partition: ${msg.record.partition()} Consume msg with key: ${msg.record.key()} and value: ${msg.record.value()}")
        if (msg.record.key() == "fakenews") {
          import akka.pattern.ask
          implicit val askTimeout = Timeout(30.seconds)
        (total ? IncrementWord(msg.record.value.toInt, id)).mapTo[Done]
      }
      batch.updated(msg.committableOffset)
      }
      .mapAsync(3)(_.commitScaladsl())
      .runWith(Sink.ignore)
  }

  createAndRunConsumerWordCount("W.1")
  createAndRunConsumerWordCount("W.2")
  createAndRunConsumerWordCount("W.3")
  createAndRunConsumerWordCount("W.4")
  createAndRunConsumerWordCount("W.5")
  createAndRunConsumerWordCount("W.6")
  createAndRunConsumerWordCount("W.7")
  createAndRunConsumerWordCount("W.8")
  createAndRunConsumerWordCount("W.9")
  createAndRunConsumerWordCount("W.10")

  sys.addShutdownHook{
    println("Got shutdown cmd from shell, about to shutdown...")
    system.terminate()
  }
}

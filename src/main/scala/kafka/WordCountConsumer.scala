package kafka

import akka.Done
import akka.actor.{ActorSystem, Props}
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Keep
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
  implicit val system = ActorSystem("WordCountConsumer")
  implicit val ec = system.dispatcher

  val total = system.actorOf(Props[TotalFake], "totalFake")

  val committerSettings = CommitterSettings(system)

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
        //println(s"$id - Offset: ${msg.record.offset()} - Partition: ${msg.record.partition()} Consume msg with key: ${msg.record.key()} and value: ${msg.record.value()}")
        if (msg.record.key().equalsIgnoreCase("fakeNews")) { //WTF WordCountProducer.fakeNewsKeyword does not work
          import akka.pattern.ask
          implicit val askTimeout: Timeout = Timeout(3.seconds)
          (total ? IncrementWord(msg.record.value.toInt, id))
            .mapTo[Done]
            .map(_ => msg.committableOffset)
        } else {
          Future(msg).map(_ => msg.committableOffset)
        }
      }
      .toMat(Committer.sink(committerSettings))(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()
  }

  def createAndRunConsumerMessageCount(id: String) = {
    Consumer.committableSource(createConsumerSettings("messagecount consumer group"), Subscriptions.topics("messagecount-output"))
      .mapAsync(1) { msg =>
        //println(s"$id - Offset: ${msg.record.offset()} - Partition: ${msg.record.partition()} Consume msg with key: ${msg.record.key()} and value: ${msg.record.value()}")
        import akka.pattern.ask
        implicit val askTimeout: Timeout = Timeout(3.seconds)
        (total ? IncrementMessage(msg.record.value.toInt, id))
          .mapTo[Done]
          .map(_ => msg.committableOffset)
      }
      .toMat(Committer.sink(committerSettings))(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()

  }

  val drainingControlW1 = createAndRunConsumerWordCount("W.1")
  val drainingControlW2 = createAndRunConsumerWordCount("W.2")
  val drainingControlM = createAndRunConsumerMessageCount("M")


  sys.addShutdownHook{
    println("Got control-c cmd from shell, about to shutdown...")
    drainingControlW1.drainAndShutdown()
    drainingControlW2.drainAndShutdown()
    drainingControlM.drainAndShutdown()
  }
}

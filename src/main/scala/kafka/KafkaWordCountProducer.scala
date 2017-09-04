package kafka

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.kafka.ProducerMessage.Message
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.{Done, NotUsed}
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object KafkaWordCountProducer extends App {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(9092, 2181, Map("offsets.topic.replication.factor" -> "1"))
  val bootstrapServers = s"localhost:${embeddedKafkaConfig.kafkaPort}"
  val InitialMsg = "initial msg in topic, required to create the topic before any consumer subscribes to it"

  val partition0 = 0

  //TODO Aha a keySerializer must be specified
  //https://github.com/akka/reactive-kafka/issues/212
  val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)

  def givenInitializedTopic(topic: String): Unit = {
    val producer = producerSettings.createKafkaProducer()
    producer.send(new ProducerRecord(topic, partition0, null: Array[Byte], InitialMsg))
    producer.close(60, TimeUnit.SECONDS)
  }

  /**
    * Produce messages to topic using specified range and return
    * a Future so the caller can synchronize consumption.
    */
  def produce(topic: String, randomText: String, settings: ProducerSettings[Array[Byte], String] = producerSettings): Future[Done] = {
    val splittedText = randomText.split(" ").toList
    val source = Source(splittedText)
      .map(each => {
        //Kafka automatically adds current time to Producer records
        val record = new ProducerRecord(topic, partition0, null: Array[Byte], each)
        Message(record, NotUsed)
      })
      .viaMat(Producer.flow(settings))(Keep.right)

    source.runWith(Sink.ignore)
  }


  givenInitializedTopic("wordcount-input")


  val randomText = TextMessage.genRandTextWithKeyword(100,100, 3, 5, 5, 10, "fakeNews")
  println("Random Text: " + randomText )


  //TODO Try with contSource
  val contSource  = Source.tick(1.second, 100.millis, TextMessage.genRandTextWithKeyword(100,100, 3, 5, 5, 10, "fakeNews"))



  //Send n messages async (via callback see ProducerStage>>setHandler) and wait for completion of all messages
  Await.result(produce("wordcount-input", randomText), 10 seconds)

 system.terminate()

}

package kafka

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.kafka.ProducerMessage.Message
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, ThrottleMode}
import akka.{Done, NotUsed}
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import scala.concurrent.Future
import scala.concurrent.duration._

object KafkaWordCountProducer extends App {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(9092, 2181, Map("offsets.topic.replication.factor" -> "1"))
  val bootstrapServers = s"localhost:${embeddedKafkaConfig.kafkaPort}"
  //TODO Needed?
  val InitialMsg = "initial msg in topic, required to create the topic before any consumer subscribes to it"

  val partition0 = 0

  //A keySerializer must be specified
  //https://github.com/akka/reactive-kafka/issues/212
  val producerSettings = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)

  def initializeTopic(topic: String): Unit = {
    val producer = producerSettings.createKafkaProducer()
    producer.send(new ProducerRecord(topic, partition0, null: Array[Byte], InitialMsg))
    producer.close(60, TimeUnit.SECONDS)
  }

  /**
    * Produce unbounded messages to the topic using the messageMap
    */
  def produce(topic: String, messageMap: Map[Int, String], settings: ProducerSettings[Array[Byte], String] = producerSettings): Future[Done] = {

    val source = Source.fromIterator(() => {
      Iterator.continually{
        val nextInt = java.util.concurrent.ThreadLocalRandom.current().nextInt(messageMap.size)
        val nextString = messageMap.getOrElse(nextInt, "N/A")
        println("Next Message: " + nextString)
        nextString
      }
    })
      .map(each => {
        //Kafka automatically adds current time to Producer records
        val record = new ProducerRecord(topic, partition0, null: Array[Byte], each)
        Message(record, NotUsed)
      })
      .throttle(10, 1.second, 10, ThrottleMode.shaping) //TODO Check outOfMemory when higher throughput
      .viaMat(Producer.flow(settings))(Keep.right)

    source.runWith(Sink.ignore)
  }

  initializeTopic("wordcount-input")
  val randomMap: Map[Int, String] = TextMessageGenerator.genRandTextWithKeyword(1000,1000, 3, 5, 5, 10, "fakeNews").split("([!?.])").toList.zipWithIndex.toMap.map(_.swap)
  produce("wordcount-input", randomMap)
}

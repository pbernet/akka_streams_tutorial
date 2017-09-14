package kafka

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.kafka.ProducerMessage.Message
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, ThrottleMode}
import akka.{Done, NotUsed}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.{NetworkException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Produce unbounded messages to the topic wordcount-input
  *
  */
object WordCountProducer extends App {
  implicit val system = ActorSystem()
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  val bootstrapServers = "localhost:9092"

  //initial msg in topic, required to create the topic before any consumer subscribes to it
  val InitialMsg = "truth"

  val partition0 = 0

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)

  def initializeTopic(topic: String): Unit = {
    val producer = producerSettings.createKafkaProducer()
    producer.send(new ProducerRecord(topic, partition0, null: String, InitialMsg))
    producer.close(60, TimeUnit.SECONDS)
  }

  def produce(topic: String, messageMap: Map[Int, String], settings: ProducerSettings[String, String] = producerSettings): Future[Done] = {

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
        val record = new ProducerRecord(topic, partition0, null: String, each)
        Message(record, NotUsed)
      })
      .throttle(1000, 1.second, 10, ThrottleMode.shaping)
      .viaMat(Producer.flow(settings))(Keep.right)

    source.runWith(Sink.ignore)
  }

  sys.addShutdownHook{
    println("About to shutdown...")
  }

  initializeTopic("wordcount-input")
  val randomMap: Map[Int, String] = TextMessageGenerator.genRandTextWithKeyword(1000,1000, 3, 5, 5, 10, "fakeNews").split("([!?.])").toList.zipWithIndex.toMap.map(_.swap)
  val doneFuture = produce("wordcount-input", randomMap)

  doneFuture.recover{
    case e: NetworkException => {
      println(s"NetworkException $e occurred - Retry...")
      produce("wordcount-input", randomMap)
    }
    case e: UnknownTopicOrPartitionException => {
      println(s"UnknownTopicOrPartitionException $e occurred - Retry...")
      produce("wordcount-input", randomMap)
    }
    case ex: RuntimeException => {
      println(s"Exception $ex occurred - Do not retry. Shutdown...")
      system.terminate()
    }
  }
}

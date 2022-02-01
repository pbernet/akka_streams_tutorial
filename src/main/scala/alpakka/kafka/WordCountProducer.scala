package alpakka.kafka

import akka.actor.ActorSystem
import akka.kafka.ProducerMessage.Message
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.ThrottleMode
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.{Done, NotUsed}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.{NetworkException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Produce unbounded text messages to the topic wordcount-input
  *
  */
object WordCountProducer extends App {
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val bootstrapServers = "localhost:29092"

  val topic = "wordcount-input"
  val fakeNewsKeyword = "fakeNews"


  // initial msg in topic, required to create the topic before any consumer subscribes to it
  val InitialMsg = "truth"

  val partition0 = 0

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)

  def initializeTopic(topic: String): Unit = {
    val producer = producerSettings.createKafkaProducer()
    producer.send(new ProducerRecord(topic, partition0, null: String, InitialMsg))
  }

  def produce(topic: String, messageMap: Map[Int, String], settings: ProducerSettings[String, String] = producerSettings): Future[Done] = {

    val source = Source.fromIterator(() => {
      Iterator.continually {
        val nextInt = java.util.concurrent.ThreadLocalRandom.current().nextInt(messageMap.size)
        val nextString = messageMap.getOrElse(nextInt, "N/A")
        println("Next Message: " + nextString)
        nextString
      }
    })
      .map(each => {
        val recordWithCurrentTimestamp = new ProducerRecord(topic, null: String, each)
        Message(recordWithCurrentTimestamp, NotUsed)
      })
      .throttle(100, 100.milli, 10, ThrottleMode.shaping)
      .viaMat(Producer.flexiFlow(settings))(Keep.right)

    source.runWith(Sink.ignore)
  }

  sys.addShutdownHook {
    println("Got control-c cmd from shell, about to shutdown...")
  }

  initializeTopic(topic)
  val randomMap: Map[Int, String] = TextMessageGenerator.genRandTextWithKeyword(1000, 1000, 3, 5, 5, 10, WordCountProducer.fakeNewsKeyword).split("([!?.])").toList.zipWithIndex.toMap.map(_.swap)
  val done = produce(topic, randomMap)

  done.recover {
    case e: NetworkException =>
      println(s"NetworkException $e occurred. Retry...")
      produce(topic, randomMap)
    case e: UnknownTopicOrPartitionException =>
      println(s"UnknownTopicOrPartitionException $e occurred. Retry...")
      produce(topic, randomMap)
    case e: RuntimeException =>
      println(s"RuntimeException $e occurred. Shutdown...")
      system.terminate()
  }
}
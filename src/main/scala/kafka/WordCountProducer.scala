package kafka

import java.util
import java.util.concurrent.{ThreadLocalRandom, TimeUnit}

import akka.actor.ActorSystem
import akka.kafka.ProducerMessage.Message
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, ThrottleMode}
import akka.{Done, NotUsed}
import org.apache.kafka.clients.producer.{Partitioner, ProducerRecord}
import org.apache.kafka.common.errors.{NetworkException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.{Cluster, PartitionInfo}

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Produce unbounded text messages to the topic wordcount-input
  *
  */
object WordCountProducer extends App {
  implicit val system = ActorSystem()
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  val bootstrapServers = "localhost:9092"

  val topic = "wordcount-input"

  //initial msg in topic, required to create the topic before any consumer subscribes to it
  val InitialMsg = "truth"

  val partition0 = 0

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)
    .withProperty("partitioner.class", "kafka.CustomPartitioner")

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
        //Use CustomPartitioner hook
        val recordWithCurrentTimestamp = new ProducerRecord(topic, null: String, each)
        Message(recordWithCurrentTimestamp, NotUsed)
      })
      .throttle(100, 100.milli, 10, ThrottleMode.shaping)
      .viaMat(Producer.flow(settings))(Keep.right)

    source.runWith(Sink.ignore)
  }

  sys.addShutdownHook{
    println("Got control-c cmd from shell, about to shutdown...")
    system.terminate()
  }

  initializeTopic(topic)
  val randomMap: Map[Int, String] = TextMessageGenerator.genRandTextWithKeyword(1000,1000, 3, 5, 5, 10, "fakeNews").split("([!?.])").toList.zipWithIndex.toMap.map(_.swap)
  val doneFuture = produce(topic, randomMap)

  doneFuture.recover{
    case e: NetworkException => {
      println(s"NetworkException $e occurred - Retry...")
      produce(topic, randomMap)
    }
    case e: UnknownTopicOrPartitionException => {
      println(s"UnknownTopicOrPartitionException $e occurred - Retry...")
      produce(topic, randomMap)
    }
    case ex: RuntimeException => {
      println(s"RuntimeException $ex occurred - Do not retry. Shutdown...")
      system.terminate()
    }
  }
}

/**
  * Example of a CustomPartitioner hook in the producer realm
  * Done like this, because here we have access to cluster.availablePartitionsForTopic
  *
  * The partitioning here gives no added value for the downstream WordCountKStreams,
  * because here a whole message is put into the partition. WordCountKStreams counts words though
  *
  */
class CustomPartitioner extends Partitioner {
  override def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    val partitionInfoList: util.List[PartitionInfo] = cluster.availablePartitionsForTopic(topic)
    val partitionCount = partitionInfoList.size
    val fakeNewsPartition = 0

    //println("CustomPartitioner received key: " + key + " and value: " + value)

    if (value.toString.contains("fakeNews")) {
      println("CustomPartitioner send message: " + value + " to fakeNewsPartition")
      fakeNewsPartition
    }
    else ThreadLocalRandom.current.nextInt(1, partitionCount) //round robin
  }

  override def close(): Unit = {
    println("CustomPartitioner: " + Thread.currentThread + " received close")
  }

  override def configure(configs: util.Map[String, _]): Unit = {
    println("CustomPartitioner received configure with configuration: " + configs)
  }
}

object CustomPartitioner {
  private def deserialize[V](objectData: Array[Byte]): V = org.apache.commons.lang3.SerializationUtils.deserialize(objectData).asInstanceOf[V]
}
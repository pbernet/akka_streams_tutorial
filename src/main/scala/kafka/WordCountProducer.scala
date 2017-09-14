package kafka

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.pattern.{Backoff, BackoffSupervisor}
import akka.stream.ActorMaterializer
import akka.util.Timeout
import kafka.WordCountProducerActor.Produce
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.{NetworkException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.serialization.StringSerializer

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

//  def produce(topic: String, messageMap: Map[Int, String], settings: ProducerSettings[String, String] = producerSettings): Future[Done] = {
//
//    val source = Source.fromIterator(() => {
//      Iterator.continually{
//        val nextInt = java.util.concurrent.ThreadLocalRandom.current().nextInt(messageMap.size)
//        val nextString = messageMap.getOrElse(nextInt, "N/A")
//        println("Next Message: " + nextString)
//        nextString
//      }
//    })
//      .map(each => {
//        //Kafka automatically adds current time to Producer records
//        val record = new ProducerRecord(topic, partition0, null: String, each)
//        Message(record, NotUsed)
//      })
//      .throttle(1000, 1.second, 10, ThrottleMode.shaping)
//      .viaMat(Producer.flow(settings))(Keep.right)
//
//    source.runWith(Sink.ignore)
//  }

  sys.addShutdownHook{
    println("About to shutdown...")
  }

  initializeTopic("wordcount-input")

  val props =  WordCountProducerActor.props()
  val producerActor = system.actorOf(props, name = "WordCountProducerActor")
  val supervisor = BackoffSupervisor.props(
    Backoff.onFailure(
      props,
      childName = "1",
      minBackoff = 1.second,
      maxBackoff = 30.seconds,
      randomFactor = 0.2
    ))

  system.actorOf(supervisor, name = s"backoff-supervisor")

  import akka.pattern.ask
  implicit val askTimeout = Timeout(30.seconds)
  val doneFuture = producerActor ? Produce

  doneFuture.recover{
    case e: NetworkException => {
      println(s"NetworkException $e occurred - Retry...")
      //producerActor ? Produce
    }
    case e: UnknownTopicOrPartitionException => {
      println(s"UnknownTopicOrPartitionException $e occurred - Retry...")
      //This kind of retry only works the first time KafkaServer is restarted
      //Maybe a Actor with SupervisionStrategy is needed here...
      //producerActor ? Produce
    }
    case ex: RuntimeException => {
      println(s"Exception $ex occurred - Do not retry. Shutdown...")
      system.terminate()
    }
  }
}

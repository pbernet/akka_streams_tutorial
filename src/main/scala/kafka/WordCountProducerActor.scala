package kafka

import akka.actor.{Actor, ActorLogging, Props}
import akka.kafka.ProducerMessage.Message
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, ThrottleMode}
import akka.{Done, NotUsed}
import kafka.WordCountProducer.{partition0, producerSettings}
import kafka.WordCountProducerActor.Produce
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.Future
import scala.concurrent.duration._

object WordCountProducerActor {
  def props()(implicit materializer: ActorMaterializer) =
    Props(classOf[WordCountProducerActor], materializer)

  final case object Produce
}


class WordCountProducerActor()(implicit materializer: ActorMaterializer) extends Actor with ActorLogging {

  override def receive: Receive = startup //initial state

  private def startup:  Receive = {
    case Produce =>
      log.info(s"Received Produce ")
      val randomMap: Map[Int, String] = TextMessageGenerator.genRandTextWithKeyword(1000,1000, 3, 5, 5, 10, "fakeNews").split("([!?.])").toList.zipWithIndex.toMap.map(_.swap)
      sender ! produce("wordcount-input", randomMap)

  }

  private def produce(topic: String, messageMap: Map[Int, String], settings: ProducerSettings[String, String] = producerSettings): Future[Done] = {

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
      .throttle(1, 1.second, 10, ThrottleMode.shaping)
      .viaMat(Producer.flow(settings))(Keep.right)

    source.runWith(Sink.ignore)
  }
}

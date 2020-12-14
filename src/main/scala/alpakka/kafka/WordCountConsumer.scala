package alpakka.kafka

import akka.actor.{ActorSystem, Props}
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerMessage, ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Sink, ZipWith}
import akka.stream.{FlowShape, Graph}
import akka.util.Timeout
import akka.{Done, NotUsed}
import alpakka.kafka.TotalFake.{IncrementMessage, IncrementWord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Consumers W.1 and W.2 consume half of the partitions each within the wordcount consumer group
  * Only one consumer will consume the "fakeNews" partition
  *
  * Consumer M is a single consumer for all the partitions in the messagecount consumer group
  *
  * Use the offset storage in Kafka:
  * https://doc.akka.io/docs/akka-stream-kafka/current/consumer.html#offset-storage-in-kafka-committing
  *
  */
object WordCountConsumer extends App {
  implicit val system = ActorSystem("WordCountConsumer")
  implicit val ec = system.dispatcher

  val total = system.actorOf(Props[TotalFake](), "totalFake")

  val committerSettings = CommitterSettings(system).withMaxBatch(1)

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
        if (msg.record.key().equalsIgnoreCase("fakeNews")) { //hardcoded because WordCountProducer.fakeNewsKeyword does not work
          import akka.pattern.ask
          implicit val askTimeout: Timeout = Timeout(3.seconds)
          (total ? IncrementWord(msg.record.value.toInt, id))
            .mapTo[Done]
            .map(_ => msg.committableOffset)
        } else {
          Future(msg).map(_ => msg.committableOffset)
        }
      }
      .via(Committer.flow(committerSettings))
      .toMat(Sink.seq)(DrainingControl.apply)
      .run()
  }

  def createAndRunConsumerMessageCount(id: String) = {
    implicit val askTimeout = Timeout(5.seconds)

    val writeFlow = Flow[ConsumerMessage.CommittableMessage[String, java.lang.Long]]
      .map(msg => IncrementMessage(msg.record.value.toInt, id))
      .ask[Done](total)

    Consumer.committableSource(createConsumerSettings("messagecount consumer group"), Subscriptions.topics("messagecount-output"))
      .via(PassThroughFlow(writeFlow, Keep.right))
      .map(_.committableOffset)
      .via(Committer.flow(committerSettings))
      .toMat(Sink.seq)(DrainingControl.apply)
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

  object PassThroughFlow {
    def apply[A, T](processingFlow: Flow[A, T, NotUsed]): Graph[FlowShape[A, (T, A)], NotUsed] =
      apply[A, T, (T, A)](processingFlow, Keep.both)

    def apply[A, T, O](processingFlow: Flow[A, T, NotUsed], output: (T, A) => O): Graph[FlowShape[A, O], NotUsed] =
      Flow.fromGraph(GraphDSL.create() { implicit builder =>
      {
        import GraphDSL.Implicits._

        val broadcast = builder.add(Broadcast[A](2))
        val zip = builder.add(ZipWith[T, A, O]((left, right) => output(left, right)))

        // format: off
        broadcast.out(0) ~> processingFlow ~> zip.in0
        broadcast.out(1)         ~>           zip.in1
        // format: on

        FlowShape(broadcast.in, zip.out)
      }
      })
  }

}

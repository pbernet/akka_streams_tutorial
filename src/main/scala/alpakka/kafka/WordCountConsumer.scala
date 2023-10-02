package alpakka.kafka

import alpakka.kafka.TotalFake.{IncrementMessage, IncrementWord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.kafka.scaladsl.Consumer.DrainingControl
import org.apache.pekko.kafka.scaladsl.{Committer, Consumer}
import org.apache.pekko.kafka.{CommitterSettings, ConsumerMessage, ConsumerSettings, Subscriptions}
import org.apache.pekko.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, ZipWith}
import org.apache.pekko.stream.{FlowShape, Graph}
import org.apache.pekko.util.Timeout
import org.apache.pekko.{Done, NotUsed}

import scala.concurrent.duration._

/**
  * Consumers W.1 and W.2 consume partitions within the "wordcount consumer group"
  * Only one consumer will consume the "fakeNews" partition
  *
  * Consumer M is a single consumer for all the partitions in the "messagecount consumer group"
  *
  * Commit offset positions explicitly to Kafka:
  * https://doc.akka.io/docs/akka-stream-kafka/current/consumer.html#offset-storage-in-kafka-committing
  *
  * Use DrainingControl:
  * https://doc.akka.io/docs/alpakka-kafka/current/consumer.html#draining-control
  */
object WordCountConsumer extends App {
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val total = system.actorOf(Props[TotalFake](), "totalFake")

  val committerSettings = CommitterSettings(system).withMaxBatch(1)

  def createConsumerSettings(group: String): ConsumerSettings[String, java.lang.Long] = {
    ConsumerSettings(system, new StringDeserializer, new LongDeserializer)
      .withBootstrapServers("localhost:29092")
      .withGroupId(group)
      // Because we use DrainingControl
      .withStopTimeout(Duration.Zero)
      // Define consumer behavior upon starting to read a partition for which it does not have a committed offset or if the committed offset it has is invalid
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      // False is the default, we are doing explicit commits
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
  }

  def createAndRunConsumerWordCount(id: String) = {
    implicit val askTimeout = Timeout(5.seconds)

    val writeFlow = Flow[ConsumerMessage.CommittableMessage[String, java.lang.Long]]
      .map(msg => IncrementWord(msg, id))
      .ask[Done](total)

    Consumer.committableSource(createConsumerSettings("wordcount consumer group"), Subscriptions.topics("wordcount-output"))
      .via(PassThroughFlow(writeFlow, Keep.right))
      .map(_.committableOffset)
      .toMat(Committer.sink(committerSettings))(DrainingControl.apply)
      .run()
  }

  def createAndRunConsumerMessageCount(id: String) = {
    implicit val askTimeout = Timeout(5.seconds)

    val writeFlow = Flow[ConsumerMessage.CommittableMessage[String, java.lang.Long]]
      .map(msg => IncrementMessage(msg, id))
      .ask[Done](total)

    Consumer.committableSource(createConsumerSettings("messagecount consumer group"), Subscriptions.topics("messagecount-output"))
      .via(PassThroughFlow(writeFlow, Keep.right))
      .map(_.committableOffset)
      .toMat(Committer.sink(committerSettings))(DrainingControl.apply)
      .run()
  }

  val drainingControlW1 = createAndRunConsumerWordCount("W.1")
  val drainingControlW2 = createAndRunConsumerWordCount("W.2")
  val drainingControlM = createAndRunConsumerMessageCount("M")


  sys.addShutdownHook {
    println("Got control-c cmd from shell, about to shutdown...")
    drainingControlW1.drainAndShutdown()
    drainingControlW2.drainAndShutdown()
    drainingControlM.drainAndShutdown()
  }

  object PassThroughFlow {
    def apply[A, T](processingFlow: Flow[A, T, NotUsed]): Graph[FlowShape[A, (T, A)], NotUsed] =
      apply[A, T, (T, A)](processingFlow, Keep.both)

    def apply[A, T, O](processingFlow: Flow[A, T, NotUsed], output: (T, A) => O): Graph[FlowShape[A, O], NotUsed] =
      Flow.fromGraph(GraphDSL.create() { implicit builder => {
        import GraphDSL.Implicits._

        val broadcast = builder.add(Broadcast[A](2))
        val zip = builder.add(ZipWith[T, A, O]((left, right) => output(left, right)))

        broadcast.out(0) ~> processingFlow ~> zip.in0
        broadcast.out(1) ~> zip.in1
        FlowShape(broadcast.in, zip.out)
      }
      })
  }
}

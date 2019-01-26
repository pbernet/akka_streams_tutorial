package alpakka.jms

import java.util.concurrent.ThreadLocalRandom

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.jms._
import akka.stream.alpakka.jms.scaladsl.{JmsConsumer, JmsConsumerControl, JmsProducer}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.{Done, NotUsed}
import com.typesafe.config.Config
import javax.jms.{ConnectionFactory, Message}
import org.apache.activemq.ActiveMQConnectionFactory
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.control.NonFatal

/**
  * Produce/Consume messages against local alpakka.env.JMSServer
  * JMSServer must be re-started manually to see the restart behaviour
  *
  * Up to 1.0-M1 there was an issue discussed here:
  * Alpakka JMS connector restart behaviour
  * https://discuss.lightbend.com/t/alpakka-jms-connector-restart-behaviour/1883
  * Fixed with 1.0-M1
  *
  * Remaining issues:
  * TODO If the ack is on the envelope, the msg remains in the queue
  * TODO When throwing "flow exceptions" Msg remain pending in the queue
  */
object ProcessingApp {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("ProcessingApp")
  implicit val ec = system.dispatcher

  //Just the ex thrown by the flow are handled here, not the re-connect exceptions
  val decider: Supervision.Decider = {
    case NonFatal(e) =>
      logger.info(s"Stream failed with: ${e.getMessage}, going to resume")
      Supervision.Resume
    case _           => Supervision.Stop
  }

  implicit val materializer = ActorMaterializer.create(ActorMaterializerSettings.create(system)
    .withDebugLogging(true)
    .withSupervisionStrategy(decider)
    , system)

  def main(args: Array[String]) {

    jmsTextMessageProducerClient(connectionFactory)

    val control: JmsConsumerControl = jmsConsumerSource
      .mapAsyncUnordered(10) {
        ackEnvelope: AckEnvelope =>
          val traceID = ackEnvelope.message.getIntProperty("TRACE_ID")

          val randomTime = ThreadLocalRandom.current.nextInt(0, 5) * 100
          logger.info(s"RECEIVED Msg with TRACE_ID: $traceID - Working for: $randomTime ms")
          val start = System.currentTimeMillis()
          while ((System.currentTimeMillis() - start) < randomTime) {
            if(randomTime >= 400) throw new RuntimeException("BOOM")
          }
          Future(ackEnvelope)
      }
      .map {
        ackEnvelope =>
          //ackEnvelope.acknowledge()  //TODO If the ack is on the envelope, the msg remains in the queue
          ackEnvelope.message.acknowledge() //If the ack is on the message, the msg disappears (as expected)
          ackEnvelope.message
      }
      .wireTap(textMessage => logger.info(s"ACK Msg with TRACE_ID: ${textMessage.getIntProperty("TRACE_ID")}"))
      .withAttributes(ActorAttributes.supervisionStrategy(decider))
      .toMat(Sink.ignore)(Keep.left)
      .run()

    watcher(control)
  }

  //The "failover:" part in the brokerURL instructs ActiveMQ to reconnect on network failure
  //This does not interfere with the new 1.0-M2 implementation
  val connectionFactory: ConnectionFactory = new ActiveMQConnectionFactory("", "", "failover:tcp://127.0.0.1:8888")

  val consumerConfig: Config = system.settings.config.getConfig(JmsConsumerSettings.configPath)
  val jmsConsumerSource: Source[AckEnvelope, JmsConsumerControl] = JmsConsumer.ackSource(
    JmsConsumerSettings(consumerConfig, connectionFactory)
      .withQueue("test-queue")
      .withSessionCount(10)
      .withBufferSize(10)
  )

  private def jmsTextMessageProducerClient(connectionFactory: ConnectionFactory) = {
    val producerConfig: Config = system.settings.config.getConfig(JmsProducerSettings.configPath)
    val jmsProducerSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(
      JmsProducerSettings(producerConfig, connectionFactory).withQueue("test-queue")
    )

    Source(1 to 20)
      .throttle(1, 1.second, 1, ThrottleMode.shaping)
      .wireTap(number => logger.info(s"SEND Msg with TRACE_ID: $number"))
      .map { number =>
        JmsTextMessage(s"Payload: ${number.toString}")
          .withProperty("TRACE_ID", number)
      }
      .runWith(jmsProducerSink)
  }

  private def watcher(jmsConsumerControl: JmsConsumerControl) = {

    val browseSource: Source[Message, NotUsed] = JmsConsumer.browse(
      JmsBrowseSettings(system, connectionFactory)
        .withQueue("test-queue")
    )

    while (true) {
      val resultBrowse: Future[immutable.Seq[Message]] = browseSource.runWith(Sink.seq)
      val pendingMsg = Await.result(resultBrowse, 600.seconds).size

      logger.info(s"Pending Msg: $pendingMsg")
      Thread.sleep(5000)
    }
  }
}


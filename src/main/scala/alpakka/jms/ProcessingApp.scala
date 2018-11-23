package alpakka.jms

import java.util.concurrent.ThreadLocalRandom

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.jms.scaladsl.{JmsConsumer, JmsProducer}
import akka.stream.alpakka.jms.{AcknowledgeMode, JmsConsumerSettings, JmsProducerSettings, JmsTextMessage}
import akka.stream.scaladsl.{RestartSource, Sink, Source}
import akka.{Done, NotUsed}
import javax.jms.{ConnectionFactory, Message, TextMessage}
import org.apache.activemq.ActiveMQConnectionFactory
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.concurrent.{Future, TimeoutException}
import scala.util.control.NonFatal

/**
  * Produce/Consume messages against local alpakka.env.JMSServer (must be re-started manually)
  *
  * Reproduces this issues:
  * Alpakka JMS connector restart behaviour
  * https://discuss.lightbend.com/t/alpakka-jms-connector-restart-behaviour/1883
  * with the current JMS connector 1.0-M1
  *
  * A workaround has been applied, which works around the problem on the JMS client level
  *
  */
object ProcessingApp {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("ProcessingApp")
  implicit val ec = system.dispatcher

  //This does not have the desired effect
  val decider: Supervision.Decider = {
    case _: TimeoutException => Supervision.Restart
    case NonFatal(e) =>
      logger.info(s"Stream failed with: ${e.getMessage}, going to resume")
      Supervision.Resume
  }

  implicit val materializer = ActorMaterializer.create(ActorMaterializerSettings.create(system)
    .withDebugLogging(true)
    .withSupervisionStrategy(decider)
    .withAutoFusing(true), system)

  def main(args: Array[String]) {

    jmsTextMessageProducerClient(connectionFactory)

    val done = jmsConsumerSourceRestartable
      .mapAsyncUnordered(10) {
        case textMessage: TextMessage =>
          val traceID = textMessage.getIntProperty("TRACE_ID")

          val randomTime = ThreadLocalRandom.current.nextInt(0, 5) * 100
          logger.info(s"RECEIVED Msg from JMS with TRACE_ID: $traceID - Working for: $randomTime ms")
          val start = System.currentTimeMillis()
          while ((System.currentTimeMillis() - start) < randomTime) {}
          Future(textMessage)
      }
      .map {
      textMessage =>
        textMessage.acknowledge()
        textMessage
      }
      .wireTap(textMessage => logger.info(s"Finished processing Msg with TRACE_ID: ${textMessage.getIntProperty("TRACE_ID")} - ack"))
      .runWith(Sink.ignore)
  }

  //Workaround: The "failover:" part in the brokerURL instructs ActiveMQ to reconnect on network failure
  val connectionFactory: ConnectionFactory = new ActiveMQConnectionFactory("", "", "failover:tcp://127.0.0.1:8888")

  //This does not have the desired effect
  val jmsConsumerSourceRestartable: Source[Message, NotUsed] = RestartSource.withBackoff(
    minBackoff = 3.seconds,
    maxBackoff = 30.seconds,
    randomFactor = 0.2
  ) { () => jmsConsumerSource
  }

  val jmsConsumerSource: Source[Message, KillSwitch] = JmsConsumer(
    JmsConsumerSettings(connectionFactory)
      .withQueue("test-queue")
      .withSessionCount(10)
      .withBufferSize(10)
      .withAcknowledgeMode(AcknowledgeMode.ClientAcknowledge)
  )

  private def jmsTextMessageProducerClient(connectionFactory: ConnectionFactory) = {
    val jmsProducerSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer(
      JmsProducerSettings(connectionFactory).withQueue("test-queue")
    )

    Source(1 to 100)
      .throttle(10, 1.second, 10, ThrottleMode.shaping)
      .map { number =>
        JmsTextMessage(s"Payload: ${number.toString}")
          .withProperty("TRACE_ID", number)
      }
      .runWith(jmsProducerSink)
  }
}


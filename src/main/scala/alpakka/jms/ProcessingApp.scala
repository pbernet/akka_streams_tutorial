package alpakka.jms

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

object ProcessingApp {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("ProcessingApp")
  implicit val ec = system.dispatcher

  //This does not have the desired effect
  val decider: Supervision.Decider = {
    case _: TimeoutException => Supervision.Restart
    case NonFatal(e) =>
      println(s"Stream failed with: ${e.getMessage}, going to resume")
      Supervision.Resume
  }

  implicit val materializer = ActorMaterializer.create(ActorMaterializerSettings.create(system)
    .withDebugLogging(true)
    .withSupervisionStrategy(decider)
    .withAutoFusing(true), system)

  def main(args: Array[String]) {

    jmsTextMessageProducerClient(connectionFactory)

    val done = jmsConsumerSourceRestartable
      .map {
        case textMessage: TextMessage =>
          val traceID = textMessage.getIntProperty("TRACE_ID")
          val text = textMessage.getText
          print(s"RECEIVED Msg from JMS with TRACE_ID: $traceID\n")
          textMessage
      }

      .map {
      textMessage =>
        print(s"Finished processing Msg with TRACE_ID: ${textMessage.getIntProperty("TRACE_ID")} - ack\n")
        print("----\n")
        textMessage.acknowledge()
        textMessage
    }
      .runWith(Sink.ignore)
  }

  //The "failover:" part in the brokerURL instructs ActiveMQ to reconnect on network failure
  //This is a workaround - see discussion:
  //https://discuss.lightbend.com/t/alpakka-jms-connector-restart-behaviour/1883/3
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
      .withTopic("test-topic")
      .withBufferSize(10)
      .withAcknowledgeMode(AcknowledgeMode.ClientAcknowledge)
  )

  private def jmsTextMessageProducerClient(connectionFactory: ConnectionFactory) = {
    val jmsProducerSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer(
      JmsProducerSettings(connectionFactory).withTopic("test-topic")
    )

    Source(1 to 10000)
      .throttle(1, 1.second, 1, ThrottleMode.shaping)
      .map { number =>
        JmsTextMessage(s"Payload: ${number.toString}")
          .withProperty("TRACE_ID", number)
      }
      .runWith(jmsProducerSink)
  }
}


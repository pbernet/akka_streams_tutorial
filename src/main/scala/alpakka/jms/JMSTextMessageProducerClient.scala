package alpakka.jms

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ThrottleMode
import akka.stream.alpakka.jms.scaladsl.JmsProducer
import akka.stream.alpakka.jms.{JmsCorrelationId, JmsProducerSettings, JmsTextMessage}
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.Config
import javax.jms.ConnectionFactory
import org.apache.activemq.ActiveMQConnectionFactory
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future
import scala.concurrent.duration._

object JMSTextMessageProducerClient {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("JMSTextMessageProducerClient")
  implicit val ec = system.dispatcher

  //The "failover:" part in the brokerURL instructs ActiveMQ to reconnect on network failure
  //This does not interfere with the new 1.0-M2 implementation
  val connectionFactory: ConnectionFactory = new ActiveMQConnectionFactory("artemis", "simetraehcapa", "failover:tcp://127.0.0.1:21616")


  def main(args: Array[String]): Unit = {
    jmsTextMessageProducerClient(connectionFactory)
  }

  private def jmsTextMessageProducerClient(connectionFactory: ConnectionFactory) = {
    val producerConfig: Config = system.settings.config.getConfig(JmsProducerSettings.configPath)
    val jmsProducerSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(
      JmsProducerSettings(producerConfig, connectionFactory).withQueue("test-queue")
    )

    Source(1 to 2000000)
      .throttle(1, 1.second, 1, ThrottleMode.shaping)
      .wireTap(number => logger.info(s"SEND Msg with TRACE_ID: $number"))
      .map { number =>
        JmsTextMessage(s"Payload: ${number.toString}")
          .withProperty("TRACE_ID", number)                //custom TRACE_ID
          .withHeader(JmsCorrelationId.create(number.toString))  //The JMS way
      }
      //.wireTap(each => println(each.getHeaders))
      .runWith(jmsProducerSink)
  }
}

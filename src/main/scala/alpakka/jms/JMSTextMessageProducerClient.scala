package alpakka.jms

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ThrottleMode
import akka.stream.alpakka.jms._
import akka.stream.alpakka.jms.scaladsl.JmsProducer
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.Config
import org.apache.activemq.ActiveMQConnectionFactory
import org.slf4j.{Logger, LoggerFactory}

import javax.jms.ConnectionFactory
import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Works together with [[ProcessingApp]]
  * Implements the new ConnectionRetrySettings/SendRetrySettings of the Alpakka JMS connector,
  * together with the failover meccano provided by ActiveMQ
  *
  */
object JMSTextMessageProducerClient {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  val connectionRetrySettings = ConnectionRetrySettings(system)
    .withConnectTimeout(10.seconds)
    .withInitialRetry(100.millis)
    .withBackoffFactor(2.0d)
    .withMaxBackoff(1.minute)
    .withMaxRetries(10)

  val sendRetrySettings = SendRetrySettings(system)
    .withInitialRetry(20.millis)
    .withBackoffFactor(1.5d)
    .withMaxBackoff(500.millis)
    .withMaxRetries(10)

  // The "failover:" part in the brokerURL instructs the ActiveMQ lib to reconnect on network failure
  // Seems to work together with the new ConnectionRetrySettings/SendRetrySettings
  val connectionFactory = new ActiveMQConnectionFactory("artemis", "simetraehcapa", "failover:tcp://127.0.0.1:21616")

  def main(args: Array[String]): Unit = {
    jmsTextMessageProducerClient(connectionFactory)
  }

  private def jmsTextMessageProducerClient(connectionFactory: ConnectionFactory) = {
    val producerConfig: Config = system.settings.config.getConfig(JmsProducerSettings.configPath)
    val jmsProducerSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(
      JmsProducerSettings(producerConfig, connectionFactory).withQueue("test-queue")
        .withConnectionRetrySettings(connectionRetrySettings)
        .withSendRetrySettings(sendRetrySettings)
        .withSessionCount(1)
    )

    Source(1 to 2000000)
      .throttle(10, 1.second, 10, ThrottleMode.shaping)
      .wireTap(number => logger.info(s"SEND Msg with TRACE_ID: $number"))
      .map { number =>
        JmsTextMessage(s"Payload: ${number.toString}")
          .withProperty("TRACE_ID", number) //custom TRACE_ID
          .withHeader(JmsCorrelationId.create(number.toString)) //The JMS way
      }
      //.wireTap(each => println(each.getHeaders))
      .runWith(jmsProducerSink)
  }
}

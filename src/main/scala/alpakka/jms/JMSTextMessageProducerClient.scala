package alpakka.jms

import com.typesafe.config.Config
import jakarta.jms.ConnectionFactory
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory
import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.ThrottleMode
import org.apache.pekko.stream.connectors.jakartams.*
import org.apache.pekko.stream.connectors.jakartams.scaladsl.JmsProducer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future
import scala.concurrent.duration.*

/**
  * Works together with [[ProcessingApp]]
  * Shows how to use ConnectionRetrySettings/SendRetrySettings of the Alpakka JMS connector
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

  val connectionFactory = new ActiveMQConnectionFactory("tcp://127.0.0.1:21616")
  connectionFactory.setUser("artemis")
  connectionFactory.setPassword("artemis")

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

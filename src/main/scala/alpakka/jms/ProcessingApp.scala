package alpakka.jms

import com.typesafe.config.Config
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream._
import org.apache.pekko.stream.connectors.jms._
import org.apache.pekko.stream.connectors.jms.scaladsl.{JmsConsumer, JmsConsumerControl, JmsProducer}
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.{Done, NotUsed}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.ThreadLocalRandom
import javax.jms.{ConnectionFactory, Message, TextMessage}
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/**
  * An Alpakka JMS client which consumes text messages from either:
  *  - Preferred:    Artemis JMS Broker on docker image, started from /docker/docker-compose.yml
  *  - Preferred:    Embedded Artemis JMS Broker [[JMSServerArtemis]], started from IDE
  *  - Experimental: Embedded ActiveMQ JMS Broker [[alpakka.env.jms.JMSServerActiveMQ]], started from IDE
  *
  * Generate text messages with [[JMSTextMessageProducerClient]]
  *
  * Features:
  *  - non deliverable messages are acknowledged and written to an error queue (so that processing resumes)
  *  - Failures in this client may be simulated by throwing random java.lang.RuntimeException: BOOM
  *    see [[ProcessingApp.simulateFaultyDeliveryToExternalSystem]]
  *  - for an example of ConnectionRetrySettings/SendRetrySettings see [[JMSTextMessageProducerClient]]
  */
object ProcessingApp {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val deciderFlow: Supervision.Decider = {
    case NonFatal(e) =>
      logger.info(s"Stream failed with: ${e.getMessage}, going to restart")
      Supervision.Restart
    case _ => Supervision.Stop
  }

  def main(args: Array[String]): Unit = {

    val control: JmsConsumerControl = jmsConsumerSource
      .mapAsyncUnordered(10)(ackEnvelope => simulateFaultyDeliveryToExternalSystem(ackEnvelope))
      .map {
        ackEnvelope =>
          // Ack this way ensures that messages are not replayed upon Broker restart
          ackEnvelope.acknowledge()
          ackEnvelope.message.acknowledge()
          ackEnvelope.message
      }
      .wireTap(textMessage => logger.info(s"ACK Msg with TRACE_ID: ${textMessage.getIntProperty("TRACE_ID")}"))
      .withAttributes(ActorAttributes.supervisionStrategy(deciderFlow))
      .toMat(Sink.ignore)(Keep.left)
      .run()

    pendingMessageWatcher(control)
  }

  // The "failover:" part in the brokerURL instructs the ActiveMQ lib to reconnect on network failure
  // Seems to work together with the new connection and send retry settings on the connector
  val connectionFactory: ConnectionFactory = new ActiveMQConnectionFactory("artemis", "artemis", "failover:tcp://127.0.0.1:21616")

  val consumerConfig: Config = system.settings.config.getConfig(JmsConsumerSettings.configPath)
  val jmsConsumerSource: Source[AckEnvelope, JmsConsumerControl] = JmsConsumer.ackSource(
    JmsConsumerSettings(consumerConfig, connectionFactory)
      .withQueue("test-queue")
      .withSessionCount(5)

      // Maximum number of messages to prefetch before applying backpressure. Default value is: 100
      // Message-by-message acknowledgement can be achieved by setting bufferSize to 0, thus
      // disabling buffering. The outstanding messages before backpressure will then be the sessionCount.
      .withBufferSize(0)
      .withAcknowledgeMode(AcknowledgeMode.ClientAcknowledge) //Default
  )

  val jmsErrorQueueSettings: JmsProducerSettings = JmsProducerSettings.create(system, connectionFactory).withQueue("test-queue-error")
  val errorQueueSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(jmsErrorQueueSettings)
  val errorQueue = Source
    .queue[JmsTextMessage](100, OverflowStrategy.backpressure, 10)
    .toMat(errorQueueSink)(Keep.left)
    .run()


  // We may do a (blocking) retry in this method to handle recoverable conditions of the external system
  private def simulateFaultyDeliveryToExternalSystem(ackEnvelope: AckEnvelope) = {
    try {
      val traceID = ackEnvelope.message.getIntProperty("TRACE_ID")
      val payload = ackEnvelope.message.asInstanceOf[TextMessage].getText
      val randomTime = ThreadLocalRandom.current.nextInt(0, 5) * 100
      logger.info(s"RECEIVED Msg with TRACE_ID: $traceID and payload: $payload - Working for: $randomTime ms")
      val start = System.currentTimeMillis()
      while ((System.currentTimeMillis() - start) < randomTime) {
        // Activate to simulate failure
        //if (randomTime >= 400) throw new RuntimeException("BOOM - simulated failure in delivery")
      }
      Future(ackEnvelope)
    } catch {
      case e@(_: Exception) =>
        handleError(ackEnvelope, e)
        throw e //Rethrow to allow next element to be processed
    }
  }

  private def handleError(ackEnvelope: AckEnvelope, e: Exception): Unit = {
    sendOriginalMessageToErrorQueue(ackEnvelope, e)
    ackEnvelope.acknowledge()
  }

  private def sendOriginalMessageToErrorQueue(ackEnvelope: AckEnvelope, e: Exception): Unit = {

    val origMessage = ackEnvelope.message.asInstanceOf[TextMessage]
    val traceID = origMessage.getIntProperty("TRACE_ID")

    val errorMessage = JmsTextMessage(origMessage.getText)
      .withHeader(JmsCorrelationId.create(traceID.toString))
      .withProperty("TRACE_ID", traceID)
      .withProperty("errorType", e.getClass.getName)
      .withProperty("errorMessage", e.getMessage + " | Cause: " + e.getCause)

    errorQueue.offer(errorMessage).map {
      case QueueOfferResult.Enqueued => logger.info(s"Enqueued Msg with TRACE_ID: $traceID in error queue")
      case QueueOfferResult.Dropped => logger.error(s"Dropped Msg with TRACE_ID: $traceID from error queue")
      case QueueOfferResult.Failure(ex) => logger.error(s"Offer failed: $ex")
      case QueueOfferResult.QueueClosed => logger.error("Source Queue closed")
    }
  }

  private def pendingMessageWatcher(jmsConsumerControl: JmsConsumerControl) = {
    val queue = jmsConsumerControl.connectorState.toMat(Sink.queue())(Keep.right).run()

    val browseSource: Source[Message, NotUsed] = JmsConsumer.browse(
      JmsBrowseSettings(system, connectionFactory)
        .withQueue("test-queue")
    )

    while (true) {
      queue.pull().foreach { each => logger.info(s"Connection state: $each") }
      val browseResult: Future[immutable.Seq[Message]] = browseSource.runWith(Sink.seq)
      val pendingMessages = Await.result(browseResult, 600.seconds)

      // Sometimes after "ungraceful shutdowns" of the JMS server or this client, there are pending messages
      // The reason for this is faulty ack handling during shutdown (messages are consumed but never acknowledged)
      // After another re-start of the JMS server or this client, these messages are consumed
      // If the shutdowns are initiated gracefully with SIGTERM, there should be no pending messages
      logger.info(s"Pending Msg: ${pendingMessages.size} first 2 elements: ${pendingMessages.take(2)}")
      Thread.sleep(5000)
    }
  }


  def logWhen(done: Future[Done]) = {
    done.onComplete {
      case Success(_) =>
        logger.info("Message successfully written to error queue")
      case Failure(e) =>
        logger.error(s"Failure while writing to error queue: ${e.getMessage}")
    }
  }
}


package alpakka.jms

import java.util.concurrent.ThreadLocalRandom

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.jms._
import akka.stream.alpakka.jms.scaladsl.{JmsConsumer, JmsConsumerControl, JmsProducer}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.{Done, NotUsed}
import com.typesafe.config.Config
import javax.jms.{ConnectionFactory, Message, TextMessage}
import org.apache.activemq.ActiveMQConnectionFactory
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/**
  * ProcessingApp consumes messages against local alpakka.env.JMSServer
  * JMSServer must be re-started manually to see the restart behaviour
  * JMSTextMessageProducerClient is a separate class
  *
  * Up to 1.0-M1 there was an issue discussed here:
  * Alpakka JMS connector restart behaviour
  * https://discuss.lightbend.com/t/alpakka-jms-connector-restart-behaviour/1883
  * This is fixed with 1.0-M2
  *
  * This example has been upcycled to demonstrate a realistic consumer scenario,
  * where faulty messages are written to an error queue.
  *
  * What is interesting:
  * Acknowledge should be done on the envelope as suggested in:
  * https://doc.akka.io/docs/alpakka/1.0-M2/jms/consumer.html#using-jms-client-acknowledgement
  * However, when run against JMS Broker Artemis, we notice that the processing stops and there
  * are remaining messages on the queue.
  * This can "fixed" by by setting the bufferSize to 0 (= Message-by-message acknowledgement)
  *
  * Start the Artemis Broker from /docker/docker-compose.yml
  *
  * Possibly related to:
  * https://github.com/akka/alpakka/issues/908
  *
  */
object ProcessingApp {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("ProcessingApp")
  implicit val ec = system.dispatcher

  //Exceptions thrown by the flow are handled here, not the re-connect exceptions
  val deciderFlow: Supervision.Decider = {
    case NonFatal(e) =>
      logger.info(s"Stream failed with: ${e.getMessage}, going to restart")
      Supervision.Restart
    case _ => Supervision.Stop
  }

  implicit val materializer = ActorMaterializer.create(ActorMaterializerSettings.create(system)
    .withDebugLogging(true), system)

  def main(args: Array[String]) {

    val control: JmsConsumerControl = jmsConsumerSource
      .mapAsyncUnordered(10) {
        ackEnvelope: AckEnvelope =>

          try {
            val traceID = ackEnvelope.message.getIntProperty("TRACE_ID")
            val randomTime = ThreadLocalRandom.current.nextInt(0, 5) * 100
            logger.info(s"RECEIVED Msg with TRACE_ID: $traceID - Working for: $randomTime ms")
            val start = System.currentTimeMillis()
            while ((System.currentTimeMillis() - start) < randomTime) {
              if (randomTime >= 400) throw new RuntimeException("BOOM") //comment out for "happy path"
            }
            Future(ackEnvelope)
          } catch {
            case e@(_: Exception) =>
              handleError(ackEnvelope, e, "Error, send this message to error queue")
              throw e //Rethrow to allow next element to be processed
          }
      }
      .map {
        ackEnvelope =>
          ackEnvelope.acknowledge()
          ackEnvelope.message
      }
      .wireTap(textMessage => logger.info(s"ACK Msg with TRACE_ID: ${textMessage.getIntProperty("TRACE_ID")}"))
      .withAttributes(ActorAttributes.supervisionStrategy(deciderFlow))
      .toMat(Sink.ignore)(Keep.left)
      .run()

    watcher(control)
  }

  //The "failover:" part in the brokerURL instructs ActiveMQ to reconnect on network failure
  //This does not interfere with the new 1.0-M2 implementation
  val connectionFactory: ConnectionFactory = new ActiveMQConnectionFactory("artemis", "simetraehcapa", "failover:tcp://127.0.0.1:21616")

  val consumerConfig: Config = system.settings.config.getConfig(JmsConsumerSettings.configPath)
  val jmsConsumerSource: Source[AckEnvelope, JmsConsumerControl] = JmsConsumer.ackSource(
    JmsConsumerSettings(consumerConfig, connectionFactory)
      .withQueue("test-queue")
      .withSessionCount(1)

      // Maximum number of messages to prefetch before applying backpressure. Default value is: 100
      // Message-by-message acknowledgement can be achieved by setting bufferSize to 0, thus
      // disabling buffering. The outstanding messages before backpressure will be the sessionCount.
      .withBufferSize(0)
      .withAcknowledgeMode(AcknowledgeMode.ClientAcknowledge)  //Default
  )

  val jmsErrorQueueSettings: JmsProducerSettings = JmsProducerSettings.create(system, connectionFactory).withQueue("test-queue-error")
  val errorQueueSink: Sink[JmsTextMessage, Future[Done]] = JmsProducer.sink(jmsErrorQueueSettings)

  private def watcher(jmsConsumerControl: JmsConsumerControl) = {

    val browseSource: Source[Message, NotUsed] = JmsConsumer.browse(
      JmsBrowseSettings(system, connectionFactory)
        .withQueue("test-queue")
    )

    while (true) {
      val resultBrowse: Future[immutable.Seq[Message]] = browseSource.runWith(Sink.seq)
      val pendingMessages = Await.result(resultBrowse, 600.seconds)

      //Check value of attribute "redeliveryCounter" in log
      logger.info(s"Pending Msg: ${pendingMessages.size} first 2 elements: ${pendingMessages.take(2)}")
      Thread.sleep(5000)
    }
  }

  private def handleError(ackEnvelope: AckEnvelope, e: Exception, msg: String): Unit = {
    logger.error(msg, e)
    sendOriginalMessageToErrorQueue(ackEnvelope, e)
    ackEnvelope.acknowledge()
  }

  private def sendOriginalMessageToErrorQueue(ackEnvelope: AckEnvelope, e: Exception): Unit = {
    val done: Future[Done] = Source.single(ackEnvelope.message).map((message: Message) => {
      JmsTextMessage(message.asInstanceOf[TextMessage].getText)
        .withProperty("errorType", e.getClass.getName)
        .withProperty("errorMessage", e.getMessage + " | Cause: " + e.getCause)
    }).runWith(errorQueueSink)
    logWhen(done)
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


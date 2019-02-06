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
import scala.util.{Failure, Success}

/**
  * Produce/Consume messages against local alpakka.env.JMSServer
  * JMSServer must be re-started manually to see the restart behaviour
  * JMSTextMessageProducerClient is a separate class
  *
  * Up to 1.0-M1 there was an issue discussed here:
  * Alpakka JMS connector restart behaviour
  * https://discuss.lightbend.com/t/alpakka-jms-connector-restart-behaviour/1883
  * Fixed with 1.0-M2
  *
  * Remaining issue for 1.0-M2:
  * *Sometimes* for yet unknown reasons messages remain pending in the queue
  * *Sometimes* after manually restarting this consumer, they get consumed
  * *Some* of the remaining messages have the JMS property JMSXDeliveryCount set, which value equals the no of runs of the Consumer
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
    case _           => Supervision.Stop
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
            if(randomTime >= 400) throw new RuntimeException("BOOM")
          }
          Future(ackEnvelope)
          } catch {
            case e @ (_ : Exception ) =>
              handleError(ackEnvelope, e, "Handle error message")
              throw e  //Rethrow to allow next element to be processed
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
  val connectionFactory: ConnectionFactory = new ActiveMQConnectionFactory("", "", "failover:tcp://127.0.0.1:21616")

  val consumerConfig: Config = system.settings.config.getConfig(JmsConsumerSettings.configPath)
  val jmsConsumerSource: Source[AckEnvelope, JmsConsumerControl] = JmsConsumer.ackSource(
    JmsConsumerSettings(consumerConfig, connectionFactory)
      .withQueue("test-queue")
      .withSessionCount(10)
      .withBufferSize(10)
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
    ackEnvelope.acknowledge // Mark as complete to skip this message, see comment above
  }

  private def sendOriginalMessageToErrorQueue(ackEnvelope: AckEnvelope, e: Exception): Unit = {
    val done: Future[Done] = Source.single(ackEnvelope.message).map((message: Message) => {
      //TODO Find a way to convert javax.jms.Message to JmsTextMessage
        JmsTextMessage("should be body of original msg")
          .withProperty("errorType", e.getClass.getName)
        .withProperty("errorMessage", e.getMessage + " | Cause: " + e.getCause)
    }).runWith(errorQueueSink)
    logWhen(done)
  }

  def logWhen(done: Future[Done]) = {
    done.onComplete {
      case Success(b) =>
        logger.info("Message successfully written to error queue")
      case Failure(e) =>
        logger.error(s"Failure while writing to error queue: ${e.getMessage}")
    }
  }
}


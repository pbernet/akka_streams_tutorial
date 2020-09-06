package alpakka.amqp

import akka.actor.ActorSystem
import akka.stream.alpakka.amqp._
import akka.stream.alpakka.amqp.scaladsl.{AmqpFlow, AmqpSource}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString
import akka.{Done, NotUsed}
import org.slf4j.{Logger, LoggerFactory}
import org.testcontainers.containers.RabbitMQContainer

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

/**
  * Inspired by:
  * https://doc.akka.io/docs/alpakka/current/amqp.html
  *
  * TODO
  * Add pub/sub example
  */
object AmqpEcho extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("AmqpEcho")
  implicit val executionContext = system.dispatcher

  val (host, port) = ("127.0.0.1", 5672)
  val queueName = "myQueue"

  val rabbitMQContainer = new RabbitMQContainer("rabbitmq:3.8")
  rabbitMQContainer.start()
  logger.info(s"Started rabbitmq on: ${rabbitMQContainer.getContainerIpAddress}:${rabbitMQContainer.getMappedPort(port)}")

  (1 to 2).par.foreach(each => roundTripSendReceive(each, rabbitMQContainer))

  def roundTripSendReceive(id: Int, rabbitMQContainer: RabbitMQContainer): Unit = {
    val mappedPort = rabbitMQContainer.getAmqpPort
    val amqpUri = s"amqp://$host:$mappedPort"
    val connectionProvider = AmqpCachedConnectionProvider(AmqpUriConnectionProvider(amqpUri))
    val queueNameFull = s"$queueName-$id"
    val queueDeclaration = QueueDeclaration(queueNameFull)

    send(id, connectionProvider, queueDeclaration, queueNameFull)
      .onComplete {
        case Success(writeResult) =>
          val noOfSentMsg = writeResult.seq.size
          logger.info(s"Client: $id successfully sent: $noOfSentMsg messages to queue: $queueNameFull. Starting receiver...")
          receive(id, connectionProvider, queueDeclaration, noOfSentMsg, queueNameFull)
        case Failure(exception) => logger.info(s"Exception during send:", exception)
      }
  }

  private def send(id: Int, connectionProvider: AmqpCachedConnectionProvider, queueDeclaration: QueueDeclaration, queueNameFull: String) = {
    val settings = AmqpWriteSettings(connectionProvider)
      .withRoutingKey(queueNameFull)
      .withDeclaration(queueDeclaration)
      .withBufferSize(10)
      .withConfirmationTimeout(200.millis)

    val amqpFlow: Flow[WriteMessage, WriteResult, Future[Done]] =
      AmqpFlow.withConfirm(settings)

    val input = Vector(s"$id-1", s"$id-2", s"$id-3", s"$id-4", s"$id-5")
    val writeResult: Future[Seq[WriteResult]] =
      Source(input)
        .map(message => WriteMessage(ByteString(message)))
        .via(amqpFlow)
        .wireTap(each => logger.debug(s"WriteResult: $each"))
        .runWith(Sink.seq)
    writeResult
  }

  private def receive(id: Int, connectionProvider: AmqpCachedConnectionProvider, queueDeclaration: QueueDeclaration, noOfSentMsg: Int, queueNameFull: String) = {
    val amqpSource: Source[ReadResult, NotUsed] =
      AmqpSource.atMostOnceSource(
        NamedQueueSourceSettings(connectionProvider, queueNameFull)
          .withDeclaration(queueDeclaration)
          .withAckRequired(false),
        bufferSize = 10
      )

    val readResult: Future[Seq[ReadResult]] =
      amqpSource
        .wireTap(each => logger.debug(s"ReadResult: $each"))
        .take(noOfSentMsg)
        .runWith(Sink.seq)

    readResult.onComplete {
      case Success(each) =>
        logger.info(s"Client: $id successfully received: ${each.seq.size} messages from queue: $queueNameFull")
        each.seq.foreach(msg => logger.debug(s"Payload: ${msg.bytes.utf8String}"))
      case Failure(exception) => logger.info(s"Exception during receive:", exception)
    }
  }
}
package alpakka.amqp

import akka.actor.ActorSystem
import akka.stream.alpakka.amqp._
import akka.stream.alpakka.amqp.scaladsl.{AmqpFlow, AmqpSource}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString
import akka.{Done, NotUsed}
import org.slf4j.{Logger, LoggerFactory}
import org.testcontainers.containers.RabbitMQContainer

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

/**
  * Inspired by:
  * https://doc.akka.io/docs/alpakka/current/amqp.html
  *
  * TODO
  * Resolve readResult completion
  * Parse readResults for content _.bytes.utf8String
  * Send to different queues
  *
  * Add pub/sub example
  */
object AmqpEcho extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val systemClient1 = ActorSystem("AmqpEchoClient1")
  val systemClient2 = ActorSystem("AmqpEchoClient2")

  val (host, port) = ("127.0.0.1", 5672)
  val queueName = "myQueue"

  val rabbitMQContainer = new RabbitMQContainer("rabbitmq:3.8")
  rabbitMQContainer.start()
  logger.info(s"Started rabbitmq on: ${rabbitMQContainer.getContainerIpAddress}:${rabbitMQContainer.getMappedPort(port)}")

  (1 to 2).par.foreach(each => clientSenderReceiver(each, systemClient1, rabbitMQContainer))

  def clientSenderReceiver(id: Int, system: ActorSystem, rabbitMQContainer: RabbitMQContainer): Unit = {
    implicit val sys = system
    implicit val ec: ExecutionContextExecutor = system.dispatcher

    val mappedPort = rabbitMQContainer.getAmqpPort
    val amqpUri = s"amqp://$host:$mappedPort"
    val connectionProvider = AmqpCachedConnectionProvider(AmqpUriConnectionProvider(amqpUri))
    val queueDeclaration = QueueDeclaration(queueName)

    val settings = AmqpWriteSettings(connectionProvider)
      .withRoutingKey(queueName)
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
        .wireTap(each => logger.info(s"WriteResult: $each"))
        .runWith(Sink.seq)

    writeResult.onComplete {
      case Success(each) =>
        logger.info(s"Successfully sent: ${each.seq.size} messages to queue: $queueName. Starting receiver...")

        val amqpSource: Source[ReadResult, NotUsed] =
          AmqpSource.atMostOnceSource(
            NamedQueueSourceSettings(connectionProvider, queueName)
              .withDeclaration(queueDeclaration)
              .withAckRequired(false),
            bufferSize = 10
          )

        val readResult: Future[Seq[ReadResult]] =
          amqpSource
            .wireTap(each => logger.info(s"ReadResult: $each"))
            .runWith(Sink.seq)

        //TODO Why is readResult not completed?
        readResult.onComplete {
          case Success(each) => logger.info(s"Successfully received: ${each.seq.size} messages from queue: $queueName")
          case Failure(exception) => logger.info(s"Exception: $exception")
        }
      case Failure(exception) => logger.info(s"Exception: $exception")
    }
  }
}

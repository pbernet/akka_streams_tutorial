package alpakka.amqp

import akka.actor.ActorSystem
import akka.stream.alpakka.amqp._
import akka.stream.alpakka.amqp.scaladsl.{AmqpFlow, AmqpSink, AmqpSource}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{KillSwitches, ThrottleMode, UniqueKillSwitch}
import akka.util.ByteString
import akka.{Done, NotUsed}
import org.slf4j.LoggerFactory
import org.testcontainers.containers.RabbitMQContainer

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

/**
  * Inspired by:
  * https://doc.akka.io/docs/alpakka/current/amqp.html
  *
  * TODO
  * Add RestartSource and shutdown
  * Simulate connection problem by falsifying port (eg with +1)
  */
object AmqpEcho extends App {
  val logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("AmqpEcho")
  implicit val executionContext = system.dispatcher

  val (host, port) = ("127.0.0.1", 5672)
  val queueName = "myQueue"

  val rabbitMQContainer = new RabbitMQContainer("rabbitmq:3.8")
  rabbitMQContainer.start()
  logger.info(s"Started RabbitMQ on: ${rabbitMQContainer.getContainerIpAddress}:${rabbitMQContainer.getMappedPort(port)}")

  (1 to 2).par.foreach(each => roundTripSendReceive(each, rabbitMQContainer))
  (1 to 2).par.foreach(each => pubSubClient(each, rabbitMQContainer))

  def roundTripSendReceive(id: Int, rabbitMQContainer: RabbitMQContainer): Unit = {
    val mappedPort = rabbitMQContainer.getAmqpPort
    val amqpUri = s"amqp://$host:$mappedPort"
    val connectionProvider = AmqpCachedConnectionProvider(AmqpUriConnectionProvider(amqpUri))

    val queueNameFull = s"$queueName-$id"
    val queueDeclaration = QueueDeclaration(queueNameFull)

    sendToQueue(id, connectionProvider, queueDeclaration, queueNameFull)
      .onComplete {
        case Success(writeResult) =>
          val noOfSentMsg = writeResult.seq.size
          logger.info(s"Client: $id successfully sent: $noOfSentMsg messages to queue: $queueNameFull. Starting receiver...")
          receiveFromQueue(id, connectionProvider, queueDeclaration, noOfSentMsg, queueNameFull)
        case Failure(exception) => logger.info(s"Exception during send:", exception)
      }
  }

  /**
    * Send messages to an "exchange" and then provide instructions to the AMQP server
    * what to do with these incoming messages. The "fanout" type of the exchange
    * enables message broadcasting to multiple consumers.
    *
    * @param id
    * @param rabbitMQContainer
    */
  def pubSubClient(id: Int, rabbitMQContainer: RabbitMQContainer) = {
    val mappedPort = rabbitMQContainer.getAmqpPort
    val amqpUri = s"amqp://$host:$mappedPort"
    val connectionProvider = AmqpCachedConnectionProvider(AmqpUriConnectionProvider(amqpUri))

    val exchangeName = s"exchange-pub-sub-$id"
    val exchangeDeclaration = ExchangeDeclaration(exchangeName, "fanout")

    receiveFromExchange(id,connectionProvider, exchangeName, exchangeDeclaration)
    sendToExchange(id, connectionProvider, exchangeName, exchangeDeclaration)
  }

  private def sendToQueue(id: Int, connectionProvider: AmqpCachedConnectionProvider, queueDeclaration: QueueDeclaration, queueNameFull: String) = {
    logger.info(s"Starting sendToQueue: $queueNameFull...")

    val settings = AmqpWriteSettings(connectionProvider)
      .withRoutingKey(queueNameFull)
      .withDeclaration(queueDeclaration)
      .withBufferSize(10)
      .withConfirmationTimeout(200.millis)

    val amqpFlow: Flow[WriteMessage, WriteResult, Future[Done]] =
      AmqpFlow.withConfirm(settings)

    val writeResult: Future[Seq[WriteResult]] =
      Source(1 to 10)
        .map(each => WriteMessage(ByteString(s"$id-$each")))
        .via(amqpFlow)
        .wireTap(each => logger.debug(s"WriteResult: $each"))
        .runWith(Sink.seq)
    writeResult
  }

  private def receiveFromQueue(id: Int, connectionProvider: AmqpCachedConnectionProvider, queueDeclaration: QueueDeclaration, noOfSentMsg: Int, queueNameFull: String) = {
    logger.info(s"Starting receiveFromQueue: $queueNameFull...")

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

  private def sendToExchange(id: Int, connectionProvider: AmqpCachedConnectionProvider, exchangeName: String, exchangeDeclaration: ExchangeDeclaration) = {
    //Wait until the receiver has registered
    Thread.sleep(1000)
    logger.info(s"Starting sendToExchange: $exchangeName...")

    val amqpSink = AmqpSink.simple(
      AmqpWriteSettings(connectionProvider)
        .withExchange(exchangeName)
        .withDeclaration(exchangeDeclaration)
    )


    val dataSender = Source(1 to 10)
      .viaMat(KillSwitches.single)(Keep.right)
      .throttle(1, 1.seconds, 1, ThrottleMode.shaping)
      .map(each => s"$id-$each")
      .wireTap(each => logger.info(s"Client: $id sending: $each"))
      .map(each => ByteString(each))
      .to(amqpSink)
      .run()
  }

  private def receiveFromExchange(id: Int, connectionProvider: AmqpCachedConnectionProvider, exchangeName: String, exchangeDeclaration: ExchangeDeclaration) = {
    logger.info(s"Starting receiveFromExchange: $exchangeName...")

    val fanoutSize = 4

    //Add the index of the source to all incoming messages, so we can distinguish which source the incoming message came from.
    val mergedSources = (0 until fanoutSize).foldLeft(Source.empty[(Int, String)]) {
      case (source, fanoutBranch) =>
        source.merge(
          AmqpSource
            .atMostOnceSource(
              TemporaryQueueSourceSettings(
                connectionProvider,
                exchangeName
              ).withDeclaration(exchangeDeclaration),
              bufferSize = 1
            )
            .wireTap(msg => logger.debug(s"Route to branch: $fanoutBranch payload: ${msg.bytes.utf8String}"))
            .map(msg => (fanoutBranch, msg.bytes.utf8String))
        )
    }

    val completion: Promise[Done] = Promise[Done]
    val mergingFlow: UniqueKillSwitch = mergedSources
      .viaMat(KillSwitches.single)(Keep.right)
      .to(Sink.fold(Set.empty[Int]) {
        case (seen, (branch, element)) =>
          if (seen.size == fanoutSize) completion.trySuccess(Done)
          logger.info(s"Client: $id-$branch received payload: $element")
          seen + branch
      })
      .run()
  }
}
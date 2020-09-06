package alpakka.amqp

import akka.actor.ActorSystem
import akka.stream.alpakka.amqp._
import akka.stream.alpakka.amqp.scaladsl.{AmqpFlow, AmqpSink, AmqpSource}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{KillSwitches, UniqueKillSwitch}
import akka.util.ByteString
import akka.{Done, NotUsed}
import org.slf4j.{Logger, LoggerFactory}
import org.testcontainers.containers.RabbitMQContainer

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

/**
  * Inspired by:
  * https://doc.akka.io/docs/alpakka/current/amqp.html
  *
  * TODO
  * Finalize pubSubClient
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
  //(1 to 1).par.foreach(each => pubSubClient(each, rabbitMQContainer))

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

  /**
    * TODO document
    *  - Publisher sends msgs to "exchange"
    *  - Subscribers subscribe to exchange
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

    //Publisher

    val amqpSink = AmqpSink.simple(
      AmqpWriteSettings(connectionProvider)
        .withExchange(exchangeName)
        .withDeclaration(exchangeDeclaration)
    )

    val dataSender: UniqueKillSwitch = Source
      .repeat("stuff")
      .wireTap(each => logger.info(s"Sending: $each"))
      .viaMat(KillSwitches.single)(Keep.right)
      .map(s => ByteString(s))
      .to(amqpSink)
      .run()

    dataSender.shutdown()


    //Subscribers

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
            .map(msg => (fanoutBranch, msg.bytes.utf8String))
        )
    }

    val completion = Promise[Done]
    val mergingFlow: UniqueKillSwitch = mergedSources
      .viaMat(KillSwitches.single)(Keep.right)
      .to(Sink.fold(Set.empty[Int]) {
        case (seen, (branch, element)) =>
          if (seen.size == fanoutSize) completion.trySuccess(Done)
          logger.info(s"Receiving: $element")
          seen + branch
      })
      .run()

    mergingFlow.shutdown()

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
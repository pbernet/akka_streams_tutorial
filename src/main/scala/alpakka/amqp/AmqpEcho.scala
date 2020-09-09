package alpakka.amqp

import akka.actor.ActorSystem
import akka.stream.alpakka.amqp._
import akka.stream.alpakka.amqp.scaladsl.{AmqpFlow, AmqpSink, AmqpSource}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{KillSwitches, UniqueKillSwitch}
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
  * Finalize pubSubClient
  * Add RestartSource
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

  //(1 to 2).par.foreach(each => roundTripSendReceive(each, rabbitMQContainer))
  (1 to 1).par.foreach(each => pubSubClient(each, rabbitMQContainer))

  def roundTripSendReceive(id: Int, rabbitMQContainer: RabbitMQContainer): Unit = {
    //TODO Simulate connection problem by falsifying port (eg with +1)
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
    *
    * Send messages to an exchange and then provide instructions to the AMQP server what to do with
    * incoming messages. We are going to use the fanout type of the exchange,
    * which enables message broadcasting to multiple consumers.
    * Exchange declaration for the sink and all of the sources.
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


    val dataSender = Source(1 to 10)
      .viaMat(KillSwitches.single)(Keep.right)
      .map(each => s"$id-$each")
      .wireTap(each => logger.info(s"Sending: $each"))
      .map(each => ByteString(each))
      .to(amqpSink)
      .run()



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

    val completion: Promise[Done] = Promise[Done]
    val mergingFlow: UniqueKillSwitch = mergedSources
      .viaMat(KillSwitches.single)(Keep.right)
      //TODO What does this do?
      .to(Sink.fold(Set.empty[Int]) {
        case (seen, (branch, element)) =>
          if (seen.size == fanoutSize) completion.trySuccess(Done)
          logger.info(s"Receiving: $element")
          seen + branch
      })
      .run()


    //TODO how to make dependent on finish receiver
//      logger.info(s"Successfully sent/received messages via: $exchangeName")
//      dataSender.shutdown()
//      mergingFlow.shutdown()
  }


  private def sendToQueue(id: Int, connectionProvider: AmqpCachedConnectionProvider, queueDeclaration: QueueDeclaration, queueNameFull: String) = {
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
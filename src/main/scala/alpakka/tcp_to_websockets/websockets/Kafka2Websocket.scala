package alpakka.tcp_to_websockets.websockets

import java.util.Locale
import java.util.concurrent.atomic.AtomicReference

import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.scaladsl.{Consumer, Transactional}
import akka.pattern.{BackoffOpts, BackoffSupervisor}
import akka.stream.RestartSettings
import akka.stream.scaladsl.{RestartSource, Sink}
import akka.util.Timeout
import alpakka.tcp_to_websockets.websockets.WebsocketClientActor.SendMessage
import alpakka.tcp_to_websockets.websockets.WebsocketConnectionStatusActor.ConnectionStatus
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.requests.IsolationLevel
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
  * Tries to deliver all messages from the Kafka "hl7-input" topic to the [[alpakka.env.WebsocketServer]]
  *
  * Uses the recovery from failure approach:
  * https://doc.akka.io/docs/alpakka-kafka/current/transactions.html#recovery-from-failure
  * to restart the kafka consumer after a websocket connection issue.
  *
  * Alpakka does currently not support the STOMP protocol, see:
  * https://github.com/akka/alpakka/issues/514
  * https://github.com/akka/alpakka/pull/856
  *
  */
class Kafka2Websocket(mappedPortKafka: Int = 9092) {
  implicit val system = ActorSystem("Kafka2Websocket")
  implicit val ec = system.dispatcher
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val bootstrapServers = s"127.0.0.1:$mappedPortKafka"

  val clientID = "1"
  val webSocketEndpoint = "ws://127.0.0.1:6002/echo"
  val (websocketClientActor, websocketConnectionStatus) = websocketClient(clientID, webSocketEndpoint)

  val streamControl = createAndRunConsumer(clientID)

  private def createConsumerSettings(group: String): ConsumerSettings[String, String] = {
    ConsumerSettings(system, new StringDeserializer , new StringDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withGroupId(group)
      //Define consumer behavior upon starting to read a partition for which it does not have a committed offset or if the committed offset it has is invalid
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString.toLowerCase(Locale.ENGLISH))
  }

  def createProducerSettings = {
    ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withBootstrapServers(bootstrapServers)
  }

  def initializeTopic(topic: String): Unit = {
    val producer = createProducerSettings.createKafkaProducer()
    producer.send(new ProducerRecord(topic, 0, null: String, "InitialMsg"))
    logger.info(s"Topic: $topic initialized")
  }

  val transactionalProducerTopic = "transactionalProducerTopic"
  initializeTopic(transactionalProducerTopic)


  def websocketClient(clientID: String, endpoint: String) = {
    val websocketConnectionStatusActor = system.actorOf(WebsocketConnectionStatusActor.props(clientID, endpoint), name = "WebsocketConnectionStatus")

    val supervisor = BackoffSupervisor.props(
      BackoffOpts.onFailure(
        WebsocketClientActor.props(clientID, endpoint, websocketConnectionStatusActor),
        childName = clientID,
        minBackoff = 1.second,
        maxBackoff = 60.seconds,
        randomFactor = 0.2
      ))

    val websocketClientActor = system.actorOf(supervisor, name = s"$clientID-backoff-supervisor")
    (websocketClientActor, websocketConnectionStatusActor)
  }

  private def createAndRunConsumer(transactionalId: String) = {

    val innerControl = new AtomicReference[Control](Consumer.NoopControl)

    val restartSettings = RestartSettings(1.second, 10.seconds, 0.2).withMaxRestarts(10, 1.minute)
    val stream = RestartSource.onFailuresWithBackoff(restartSettings ) { () =>
      Transactional
        .source(createConsumerSettings("hl7-input consumer group"), Subscriptions.topics("hl7-input"))
        .mapAsync(1) { msg =>
          logger.info(s"TransactionalID: $transactionalId - Offset: ${msg.record.offset()} - Partition: ${msg.record.partition()} Consume msg with key: ${msg.record.key()} and value: ${printableShort(msg.record.value())}")

          import akka.pattern.ask
          implicit val askTimeout: Timeout = Timeout(10.seconds)

          // With this blocking behaviour we avoid loosing messages when the websocket connection is down.
          // However, the current in-flight message will be lost.
          // To not loose any messages, we may:
          //  - In WebSocketClient check the async ACK before the commit below
          //  - use the STOMP protocol, see: stomp.github.io
          val isConnectedFuture = (websocketConnectionStatus ? ConnectionStatus).mapTo[Boolean]
          val isConnected = Await.result(isConnectedFuture, 10.seconds)

          if (isConnected) {
            websocketClientActor ! SendMessage(printableShort(msg.record.value()))
            Future(msg)
          } else {
            logger.warn("WebSocket connection failure, going to restart Kafka consumer")
            throw new RuntimeException("WebSocket connection failure")
          }
        }
        .map { msg =>
          ProducerMessage.single(new ProducerRecord(transactionalProducerTopic, msg.record.key, msg.record.value), msg.partitionOffset)
        }
        // side effect out the `Control` materialized value because it can't be propagated through the `RestartSource`
        .mapMaterializedValue(c => innerControl.set(c))
        .via(Transactional.flow(createProducerSettings, transactionalId))
  }

    stream.runWith(Sink.ignore)
    innerControl
  }

  // The HAPI parser needs /r as segment terminator, but this is not printable
  private def printable(message: String): String = {
    message.replace("\r", "\n")
  }

  private def printableShort(message: String): String = {
    printable(message).take(20).concat("...")
  }

  sys.ShutdownHookThread{
    logger.info("Got control-c cmd from shell or SIGTERM, about to shutdown...")
    Await.result(streamControl.get.shutdown(), 10.seconds)
  }
}

object Kafka2Websocket extends App {
  val server = new Kafka2Websocket()
  def apply(mappedPortKafka: Int) = new Kafka2Websocket(mappedPortKafka)
}
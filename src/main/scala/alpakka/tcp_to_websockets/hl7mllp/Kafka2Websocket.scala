package alpakka.tcp_to_websockets.hl7mllp

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.model.ws._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import akka.pattern.{BackoffOpts, BackoffSupervisor}
import akka.stream.scaladsl.{Keep, Sink}
import akka.util.Timeout
import alpakka.tcp_to_websockets.hl7mllp.WebsocketClientActor.SendMessage
import alpakka.tcp_to_websockets.hl7mllp.WebsocketConnectionStatus.ConnectionStatus
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
  * Consumer M is a single consumer for all the partitions in the hl7-input consumer group
  *
  * Use the offset storage in Kafka:
  * https://doc.akka.io/docs/akka-stream-kafka/current/consumer.html#offset-storage-in-kafka-committing
  *
  * TODO Add transactional and restart behaviour
  *
  * TODO Add websocket protocol eg. STOMP https://github.com/akka/alpakka/issues/514
  *
  */
object Kafka2Websocket extends App {
  implicit val system = ActorSystem("Kafka2Websocket")
  implicit val ec = system.dispatcher
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val committerSettings = CommitterSettings(system)
  val bootstrapServers = "localhost:9092"

  val printSink = createEchoPrintSink()
  val (address, port) = ("127.0.0.1", 6002)

  val (websocketClientActor, websocketConnectionStatus) = websocketClient()

  createAndRunConsumerMessageCount("M")

  private def createConsumerSettings(group: String): ConsumerSettings[String, String] = {
    ConsumerSettings(system, new StringDeserializer , new StringDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withGroupId(group)
      //Define consumer behavior upon starting to read a partition for which it does not have a committed offset or if the committed offset it has is invalid
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  }


  def websocketClient() = {

    val endpoint = "ws://127.0.0.1:6002/echo"
    val id =  "anID"

    val websocketConnectionStatusActor = system.actorOf(WebsocketConnectionStatus.props(id, endpoint), name = "WebsocketConnectionStatus")

    val supervisor = BackoffSupervisor.props(
      BackoffOpts.onFailure(
        WebsocketClientActor.props(id, endpoint, websocketConnectionStatusActor),
        childName = id,
        minBackoff = 1.second,
        maxBackoff = 60.seconds,
        randomFactor = 0.2
      ))

    val websocketClientActor = system.actorOf(supervisor, name = s"$id-backoff-supervisor")
    (websocketClientActor, websocketConnectionStatusActor)
  }






  private def createAndRunConsumerMessageCount(id: String) = {

    // For at least-once delivery
    // https://doc.akka.io/docs/alpakka-kafka/current/consumer.html#offset-storage-in-kafka-committing
    val control =
      Consumer
        .committableSource(createConsumerSettings("hl7-input consumer group"), Subscriptions.topics("hl7-input"))
        .mapAsync(1) { msg =>
          //        println(s"$id - Offset: ${msg.record.offset()} - Partition: ${msg.record.partition()} Consume msg with key: ${msg.record.key()} and value: ${printableShort(msg.record.value())}")


          import akka.pattern.ask
          implicit val askTimeout: Timeout = Timeout(10.seconds)

          val isConnected = (websocketConnectionStatus ? ConnectionStatus).mapTo[Boolean]
          val isConnectedResult = Await.result(isConnected, 10.seconds)

          if (isConnectedResult) {
            // TODO Change to ask with "Done"
            websocketClientActor ! SendMessage(printableShort(msg.record.value()))
            Future(msg).map(_ => msg.committableOffset)
          } else {
            //TODO Add Retry logic, so we are not loosing messages. Can we add a decider to restart?
            logger.warn("Not connected")
            throw new RuntimeException("BOOM")
            //Future(msg).map(_ => msg.committableOffset)
          }
        }
        .via(Committer.flow(committerSettings.withMaxBatch(1)))
        .toMat(Sink.seq)(Keep.both)
        .mapMaterializedValue(DrainingControl.apply)
        .run()
  }


  // The HAPI parser needs /r as segment terminator, but this is not printable
  private def printable(message: String): String = {
    message.replace("\r", "\n")
  }

  private def printableShort(message: String): String = {
    printable(message).take(20).concat("...")
  }

  private def createEchoPrintSink(): Sink[Message, Future[Done]] = {
    Sink.foreach {
      //see https://github.com/akka/akka-http/issues/65
      case TextMessage.Strict(text) => println(s"Client received TextMessage.Strict: $text")
      case TextMessage.Streamed(textStream) => textStream.runFold("")(_ + _).onComplete(value => println(s"Client received TextMessage.Streamed: ${printableShort(value.get)}"))
      case BinaryMessage.Strict(binary) => //do nothing
      case BinaryMessage.Streamed(binaryStream) => binaryStream.runWith(Sink.ignore)
    }
  }
  sys.addShutdownHook{
    //TODO Do graceful shutdown
    println("Got control-c cmd from shell, about to shutdown...")
  }
}
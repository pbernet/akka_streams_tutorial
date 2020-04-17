package alpakka.tcp_to_websockets.hl7mllp

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.ws._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueue}
import akka.stream.{OverflowStrategy, QueueOfferResult}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.{Future, Promise}

/**
  * Consumer M is a single consumer for all the partitions in the hl7-input consumer group
  *
  * Use the offset storage in Kafka:
  * https://doc.akka.io/docs/akka-stream-kafka/current/consumer.html#offset-storage-in-kafka-committing
  *
  * TODO Add transactional and restart behaviour
  *
  */
object Kafka2Websocket extends App {
  implicit val system = ActorSystem("Kafka2Websocket")
  implicit val ec = system.dispatcher

  val committerSettings = CommitterSettings(system)
  val bootstrapServers = "localhost:9092"

  val printSink = createEchoPrintSink()
  val (address, port) = ("127.0.0.1", 6002)
  val sourceQueue = singleWebSocketRequestSourceQueueClient(1, address, port)

  createAndRunConsumerMessageCount("M")

  private def createConsumerSettings(group: String): ConsumerSettings[String, String] = {
    ConsumerSettings(system, new StringDeserializer , new StringDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withGroupId(group)
      //Define consumer behavior upon starting to read a partition for which it does not have a committed offset or if the committed offset it has is invalid
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  }

  def singleWebSocketRequestSourceQueueClient(id: Int, address: String, port: Int) = {

    val (source, sourceQueue) = {
      val p = Promise[SourceQueue[Message]]
      val s = Source.queue[Message](100, OverflowStrategy.backpressure).mapMaterializedValue(m => {
        p.trySuccess(m)
        m
      })
      (s, p.future)
    }

    val webSocketNonReusableFlow = Flow.fromSinkAndSourceMat(printSink, source)(Keep.right)

    val (upgradeResponse, sourceQueueWithComplete) =
      Http().singleWebSocketRequest(WebSocketRequest(s"ws://$address:$port/echo"), webSocketNonReusableFlow)

    val connected = handleUpgrade(upgradeResponse)

    connected.onComplete(done => println(s"Client: $id singleWebSocketRequestSourceQueueClient connected: $done"))
    sourceQueueWithComplete.watchCompletion().onComplete(closed => println(s"Client: $id singleWebSocketRequestSourceQueueClient closed: $closed"))

    sourceQueue

  }

  private def handleUpgrade(upgradeResponse: Future[WebSocketUpgradeResponse]) = {
    upgradeResponse.map { upgrade =>
      // status code 101 (Switching Protocols) indicates that server support WebSockets
      if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
        Done
      } else {
        throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
      }
    }
  }

  private def createAndRunConsumerMessageCount(id: String) = {
    Consumer.committableSource(createConsumerSettings("hl7-input consumer group"), Subscriptions.topics("hl7-input"))
      .mapAsync(1) { msg =>
        println(s"$id - Offset: ${msg.record.offset()} - Partition: ${msg.record.partition()} Consume msg with key: ${msg.record.key()} and value: ${printableShort(msg.record.value())}")
        sendToWebsocket(printable(msg.record.value()))
        //TODO Make commit depend on outcome
        Future(msg).map(_ => msg.committableOffset)
      }
      .toMat(Committer.sink(committerSettings))(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()
  }

  private def sendToWebsocket(messageText: String) = {
    val message = TextMessage.Strict(messageText)
    sourceQueue.flatMap { queue =>
      queue.offer(message: Message).map {
        case QueueOfferResult.Enqueued => println(s"enqueued: ${printableShort(message.text)}")
        case QueueOfferResult.Dropped => println(s"dropped: ${printableShort(message.text)}")
        case QueueOfferResult.Failure(ex) => println(s"Offer failed: $ex")
        case QueueOfferResult.QueueClosed => println("Source queue closed")
      }
    }
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
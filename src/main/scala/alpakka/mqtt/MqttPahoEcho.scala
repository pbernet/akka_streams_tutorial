package alpakka.mqtt

import akka.Done
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.mqtt._
import akka.stream.alpakka.mqtt.scaladsl.{MqttSink, MqttSource}
import akka.stream.scaladsl._
import akka.util.ByteString
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.Seq
import scala.collection.parallel.CollectionConverters._
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.sys.process.Process


/**
  * Roundtrip with the Alpakka MQTT connector based on Eclipse Paho,
  * which works only via tcp.
  * Implements Publisher/Subscriber client(s), which handle
  * initial connection failures as well as subsequent connection failures.
  *
  * Doc:
  * https://doc.akka.io/docs/alpakka/current/mqtt.html
  * https://akka.io/alpakka-samples/mqtt-to-kafka/example.html
  * https://akka.io/alpakka-samples/mqtt-to-kafka/full-source.html
  *
  * Prerequisite:
  * Start the docker MQTT Broker from: /docker/docker-compose.yml
  * eg by cmd line: docker-compose up -d mosquitto
  *
  * Simulate failure scenarios by manually re-starting mosquitto container:
  * docker-compose down
  * docker-compose up -d mosquitto
  * OR
  * docker-compose restart mosquitto
  */
object MqttPahoEcho extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()
  import system.dispatcher

  val (host, port) = ("127.0.0.1", 1883)

  val connectionSettings = MqttConnectionSettings(
    s"tcp://$host:$port",
    "N/A",
    new MemoryPersistence
    // One might think that this setting should be set to `true`. However, for yet unknown reasons
    // setting this to `false` works better (especially in case of lost subscriber connections).
  ).withAutomaticReconnect(false)

  val topic = "myTopic"

  (1 to 1).par.foreach(each => clientPublisher(each))
  (1 to 2).par.foreach(each => clientSubscriber(each))
  browserClientAdminConsole()

  def clientPublisher(clientId: Int)= {
    val messages = (0 to 100).flatMap(i => Seq(MqttMessage(topic, ByteString(s"$clientId-$i"))))

    val sink = wrapWithRestartSink(
      MqttSink(connectionSettings.withClientId(s"Pub: $clientId"), MqttQoS.AtLeastOnce))

    Source(messages)
      .throttle(1, 1.second, 1, ThrottleMode.shaping)
      .wireTap(each => logger.info(s"Pub: $clientId sending payload: ${each.payload.utf8String}"))
      .runWith(sink)
  }

  def clientSubscriber(clientId: Int): Unit= {
    // Wait with the subscribe to show the behaviour reading of the "last known good value" of retained messages
    Thread.sleep(5000)
    val subscriptions = MqttSubscriptions.create(topic, MqttQoS.atLeastOnce)

    val subscriberSource =
      MqttSource.atMostOnce(connectionSettings.withClientId(s"Sub: $clientId"), subscriptions, 8)

    val (subscribed, streamCompletion) = subscriberSource
      .wireTap(each => logger.info(s"Sub: $clientId received payload: ${each.payload.utf8String}"))
      .toMat(Sink.ignore)(Keep.both)
      .run()

    subscribed.onComplete(each => logger.info(s"Sub: $clientId subscribed: $each"))

    streamCompletion
      .recover {
        case ex =>
          logger.error(s"Sub stream failed with: ${ex.getCause} retry...")
          clientSubscriber(clientId)
      }
  }


  /**
    * Wrap the Source with restart logic and expose an equivalent materialized value
    */
  private def wrapWithRestartSource[M](source: => Source[M, Future[Done]]): Source[M, Future[Done]] = {
    val fut = Promise[Done]()
    val restartSettings = RestartSettings(1.second, 10.seconds, 0.2).withMaxRestarts(10, 1.minute)
    RestartSource.withBackoff(restartSettings) {
      () => source.mapMaterializedValue(mat => fut.completeWith(mat))
    }.mapMaterializedValue(_ => fut.future)
  }

  /**
    * Wrap the Sink with restart logic and expose an equivalent materialized value
    */
  private def wrapWithRestartSink[M](sink: => Sink[M, Future[Done]]): Sink[M, Future[Done]] = {
    val fut = Promise[Done]()
    val restartSettings = RestartSettings(1.second, 10.seconds, 0.2).withMaxRestarts(10, 1.minute)
    RestartSink.withBackoff(restartSettings) {
      () => sink.mapMaterializedValue(mat => fut.completeWith(mat))
    }.mapMaterializedValue(_ => fut.future)
  }

  private def browserClientAdminConsole() = {
    val os = System.getProperty("os.name").toLowerCase
    if (os == "mac os x") Process(s"open http://localhost:8090").!
  }
}

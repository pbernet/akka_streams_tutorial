package alpakka.mqtt

import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.event.Logging
import org.apache.pekko.stream._
import org.apache.pekko.stream.connectors.mqtt._
import org.apache.pekko.stream.connectors.mqtt.scaladsl.{MqttSink, MqttSource}
import org.apache.pekko.stream.scaladsl._
import org.apache.pekko.util.ByteString
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.parallel.CollectionConverters._
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.sys.process.{Process, stringSeqToProcess}


/**
  * MQTT 3.1.1 echo flow based on Eclipse Paho
  * Shows Publisher/Subscriber client(s), which handle
  * initial connection failures as well as subsequent connection failures
  * See also: [[MqttEcho]]
  *
  * Doc:
  * https://pekko.apache.org/docs/pekko-connectors/current/mqtt.html
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
    "",
    new MemoryPersistence
  )
    // Setting this to `true` does not work as expected
    // Instead we use RestartSource.withBackoff to reconnect in the publisher/subscriber
    .withAutomaticReconnect(false)

  val topic = "myTopic"

  (1 to 1).par.foreach(each => clientPublisher(each))
  (1 to 2).par.foreach(each => clientSubscriber(each))
  browserClientAdminConsole()

  def clientPublisher(clientId: Int)= {
    val messages = (0 to 100).flatMap(i => Seq(MqttMessage(topic, ByteString(s"$clientId-$i"))))

    val publisherSink = wrapWithRestartSink(
      MqttSink(connectionSettings.withClientId(s"Pub: $clientId"), MqttQoS.AtLeastOnce))

    Source(messages)
      .throttle(1, 1.second, 1, ThrottleMode.shaping)
      .wireTap(each => logger.info(s"Pub: $clientId sending payload: ${each.payload.utf8String}"))
      .runWith(publisherSink)
  }

  def clientSubscriber(clientId: Int): Unit= {
    // Delay the subscription to get a "last known good value" eg 6
    Thread.sleep(5000)
    val subscriptions = MqttSubscriptions.create(topic, MqttQoS.atLeastOnce)

    val subscriberSource = wrapWithRestartSource(
      MqttSource.atMostOnce(connectionSettings.withClientId(s"Sub: $clientId"), subscriptions, 8))

    val (subscribed, streamCompletion) = subscriberSource
      .wireTap(msg => logger.info(s"Sub: $clientId received payload: ${msg.payload.utf8String}"))
      .wireTap(msg => logger.debug(s"Sub: $clientId received payload: ${msg.payload.utf8String}. Details: ${msg.toString()}"))
      .toMat(Sink.ignore)(Keep.both)
      .run()

    subscribed.onComplete(each => logger.info(s"Sub: $clientId subscribed: $each"))
  }

  private def wrapWithRestartSource[M](source: => Source[M, Future[Done]]): Source[M, Future[Done]] = {
    val fut = Promise[Done]()
    val restartSettings = RestartSettings(1.second, 10.seconds, 0.2).withMaxRestarts(10, 1.minute)
    RestartSource.withBackoff(restartSettings) {
      () => source.mapMaterializedValue(mat => fut.completeWith(mat))
    }.mapMaterializedValue(_ => fut.future)
  }

  private def wrapWithRestartSink[M](sink: => Sink[M, Future[Done]]): Sink[M, Future[Done]] = {
    val fut = Promise[Done]()
    val logSettings = RestartSettings.LogSettings(Logging.DebugLevel).withCriticalLogLevel(Logging.InfoLevel, 1)

    val restartSettings = RestartSettings(1.second, 10.seconds, 0.2).withMaxRestarts(10, 1.minute).withLogSettings(logSettings)
    RestartSink.withBackoff(restartSettings) {
      () => sink.mapMaterializedValue(mat => fut.completeWith(mat))
    }.mapMaterializedValue(_ => fut.future)
  }

  // No User/PW needed
  private def browserClientAdminConsole() = {
    val os = System.getProperty("os.name").toLowerCase
    val url = "http://localhost:8090"
    if (os == "mac os x") Process(s"open $url").!
    else if (os == "windows 10") Seq("cmd", "/c", s"start $url").!
  }
}

package alpakka.mqtt

import akka.Done
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.mqtt._
import akka.stream.alpakka.mqtt.scaladsl.{MqttSink, MqttSource}
import akka.stream.scaladsl._
import akka.util.ByteString
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.slf4j.LoggerFactory

import scala.collection.immutable.Seq
import scala.collection.parallel.CollectionConverters._
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.sys.process.Process


/**
  * Roundtrip with the Alpakka connector based on Eclipse Paho
  * Assumes that we have a tcp connection
  *
  * Doc:
  * https://doc.akka.io/docs/alpakka/current/mqtt.html
  * https://akka.io/alpakka-samples/mqtt-to-kafka/example.html
  * https://akka.io/alpakka-samples/mqtt-to-kafka/full-source.html
  *
  * Prerequisite:
  * Start the docker MQTT broker from: /docker/docker-compose.yml
  * eg by cmd line: docker-compose up -d mosquitto
  *
  * Works, but has recover issues both on startup and during operation
  *
  */
object MqttPahoEcho extends App {
  val logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("MqttPahoEcho")
  implicit val executionContext = system.dispatcher

  val (host, port) = ("127.0.0.1", 1883)

  val connectionSettings = MqttConnectionSettings(
    s"tcp://$host:$port",
    "N/A",
    new MemoryPersistence
  ).withAutomaticReconnect(true)

  val topic = "myTopic"

  (1 to 1).par.foreach(each => clientPublisher(each))
  (1 to 2).par.foreach(each => clientSubscriber(each))
  browserClientAdminConsole()

  def clientPublisher(clientId: Int)= {
    val messages = (0 to 100).flatMap(i => Seq(MqttMessage(topic, ByteString(s"$clientId-$i"))))

    val sink: Sink[MqttMessage, Future[Done]] =
      MqttSink(connectionSettings.withClientId(s"Pub: $clientId"), MqttQoS.AtLeastOnce)

    Source(messages)
      .throttle(1, 1.second, 1, ThrottleMode.shaping)
      .wireTap(each => logger.info(s"Pub: $clientId sending payload: ${each.payload.utf8String}"))
      .runWith(sink)
  }

    def clientSubscriber(clientId: Int)= {
      // Wait with the subscribe to get a "last known good value"
      Thread.sleep(5000)
      val subscriptions = MqttSubscriptions.create(topic, MqttQoS.atLeastOnce)

      val restartingMqttSource = wrapWithAsRestartSource(
        MqttSource.atMostOnce(connectionSettings.withClientId(s"Sub: $clientId"), subscriptions, 8))

      val (subscribed, streamCompletion) = restartingMqttSource
        .wireTap(each => logger.info(s"Sub: $clientId received payload: ${each.payload.utf8String}"))
      .toMat(Sink.ignore)(Keep.both)
      .run()

      subscribed.onComplete(each => logger.info(s"Sub: $clientId subscribed: $each"))

      //TODO We do not get a signal when the connection to the broker is lost
      streamCompletion
        .recover {
          case exception =>
            exception.printStackTrace()
            null
        }
        .foreach(_ => system.terminate())
    }



  /**
    * Wrap a source with restart logic and expose an equivalent materialized value.
    * Tries to restart clientSubscriber on initial connection problem, but has not the desired effect
    */
  private def wrapWithAsRestartSource[M](source: => Source[M, Future[Done]]): Source[M, Future[Done]] = {
    val fut = Promise[Done]()
    val restartSettings = RestartSettings(1.second, 10.seconds, 0.2).withMaxRestarts(10, 1.minute)
    RestartSource.withBackoff(restartSettings) {
      () => source.mapMaterializedValue(mat => fut.completeWith(mat))
    }.mapMaterializedValue(_ => fut.future)
  }

  private def browserClientAdminConsole() = {
    val os = System.getProperty("os.name").toLowerCase
    if (os == "mac os x") Process(s"open http://localhost:8080").!
  }
}

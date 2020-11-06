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
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}


/**
  * Doc:
  * https://doc.akka.io/docs/alpakka/current/mqtt.html
  * https://akka.io/alpakka-samples/mqtt-to-kafka/example.html
  * https://akka.io/alpakka-samples/mqtt-to-kafka/full-source.html
  *
  * Prerequisite:
  *  - Start the docker MQTT broker from: /docker/docker-compose.yml
  *     eg by cmd line: docker-compose up -d mosquitto
  *
  * Works, but recover issues both on startup and during operation
  *
  */
object MqttPahoEcho extends App {
  val logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("MqttPahoEcho")
  implicit val executionContext = system.dispatcher

  val (host, port) = ("127.0.0.1", 1883)

  val connectionSettings = MqttConnectionSettings(
    s"tcp://$host:$port",
    "test-scala-client",
    new MemoryPersistence
  ).withAutomaticReconnect(true)

  val topic = "source-spec/topic"

  (1 to 1).par.foreach(each => clientPublisher(each))
  (1 to 1).par.foreach(each => clientSubscriber())

  def clientPublisher(each: Int)= {
    val messages = (0 to 100).flatMap(i => Seq(MqttMessage(topic, ByteString(s"$each-$i"))))

    val sink: Sink[MqttMessage, Future[Done]] =
      MqttSink(connectionSettings, MqttQoS.AtLeastOnce)

    Source(messages)
      .throttle(1, 1.second, 1, ThrottleMode.shaping)
      .wireTap(each => logger.info(s"Sending: ${each.payload.utf8String}"))
      .runWith(sink)
  }

    def clientSubscriber()= {
      //Wait with the Subscribe to get a "last known good value"
      Thread.sleep(5000)
      val subscriptions = MqttSubscriptions.create(topic, MqttQoS.atLeastOnce)

      val restartingMqttSource = wrapWithAsRestartSource(
        MqttSource.atMostOnce(connectionSettings.withClientId("source-spec/source"), subscriptions, 8))

      val (subscribed, streamCompletion) = restartingMqttSource
        .wireTap(each => logger.info("Received: " + each.payload.utf8String))
      .toMat(Sink.ignore)(Keep.both)
      .run()

      subscribed.onComplete(each => logger.info(s"Subscribed: $each"))

      //TODO We do not get a signal when the connection to the broker is lost
      streamCompletion
        .recover {
          case exception =>
            exception.printStackTrace()
            null
        }
        .foreach(_ => system.terminate)
    }



  /**
    * Wrap a source with restart logic and expose an equivalent materialized value.
    * Tries to restart clientSubscriber on initial connection problem, but has not the desired effect
    */
  private def wrapWithAsRestartSource[M](source: => Source[M, Future[Done]]): Source[M, Future[Done]] = {
    val fut = Promise[Done]
    RestartSource.withBackoff(1000.millis, 5.seconds, randomFactor = 0.2d, maxRestarts = 5) {
      () => source.mapMaterializedValue(mat => fut.completeWith(mat))
    }.mapMaterializedValue(_ => fut.future)
  }
}

package alpakka.mqtt

import akka.actor.ActorSystem
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.alpakka.mqtt.streaming.scaladsl.{ActorMqttClientSession, Mqtt}
import akka.stream.scaladsl.{Keep, Sink, Source, SourceQueueWithComplete, Tcp}
import akka.stream.{OverflowStrategy, ThrottleMode}
import akka.util.ByteString
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.DurationInt

/**
  * Inspired by:
  * https://doc.akka.io/docs/alpakka/current/mqtt-streaming.html
  *
  * Prerequisite:
  *  - Start the docker MQTT broker from: /docker/docker-compose.yml
  *     eg by cmd line: docker-compose up -d mosquitto
  *
  */
object MqttEcho extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val systemClient1 = ActorSystem("MqttEchoClient1")
  val systemClient2 = ActorSystem("MqttEchoClient2")

  val (host, port) = ("127.0.0.1", 1883)

  (1 to 1).par.foreach(each => clientPublisher(each, systemClient1, host, port))
  (1 to 2).par.foreach(each => clientSubscriber(each, systemClient2, host, port))

  def clientPublisher(id: Int, system: ActorSystem, host: String, port: Int): Unit = {
    implicit val sys = system
    implicit val ec: ExecutionContextExecutor = system.dispatcher

    val topic = "myTopic"
    val clientId = s"pub-$id"
    val pub = client(clientId, sys, host, port)

    pub.commands.offer(Command(Connect(clientId, ConnectFlags.CleanSession)))

    Source(1 to 100)
      .throttle(
        elements = 1,
        per = 1.second,
        maximumBurst = 1,
        mode = ThrottleMode.shaping
      )
      .map { each =>
        //On the server each new retained message overwrites the previous one
        val publish = Publish(ControlPacketFlags.RETAIN | ControlPacketFlags.QoSAtLeastOnceDelivery, topic, ByteString(each.toString))

        logger.info(s"Client: $clientId send payload: ${publish.payload.utf8String}")
        pub.session ! Command(publish)
      }.runWith(Sink.ignore)
  }

  def clientSubscriber(id: Int, system: ActorSystem, host: String, port: Int): Unit = {
    implicit val sys = system
    implicit val ec: ExecutionContextExecutor = system.dispatcher

    val topic = "myTopic"
    val clientId = s"sub-$id"
    val sub = client(clientId, sys, host, port)

    sub.commands.offer(Command(Connect(clientId, ConnectFlags.CleanSession)))

    //Wait with the Subscribe to get a "last known good value" eg 6
    Thread.sleep(5000)
    val topicFilters: Seq[(String, ControlPacketFlags)] = List((topic, ControlPacketFlags.QoSAtMostOnceDelivery))
    logger.info(s"Client: $clientId send Subscribe for topic: $topic")
    sub.commands.offer(Command(Subscribe(topicFilters)))
  }


  private def client(connectionId: String, system: ActorSystem, host: String, port: Int): MqttClient = {
    implicit val sys = system
    implicit val ec: ExecutionContextExecutor = system.dispatcher

    logger.info(s"Client: $connectionId starting...")

    val settings = MqttSessionSettings()
    val clientSession = ActorMqttClientSession(settings)

    val connection = Tcp().outgoingConnection(host, port)

    //TODO When there is no tcp connection: Why does the connection not throw an exception?
    //https://discuss.lightbend.com/t/alpakka-mqtt-streaming-client-does-not-complain-when-there-is-no-tcp-connection/7113
    val mqttFlow =
      Mqtt
        .clientSessionFlow(clientSession, ByteString(connectionId))
        .join(connection)

    val commands = {
      Source
        .queue(10, OverflowStrategy.backpressure,10)
        .via(mqttFlow)
        //Only the Publish events are interesting
        .collect { case Right(Event(p: Publish, _)) => p }
        .wireTap(event => logger.info(s"Client: $connectionId received payload: ${event.payload.utf8String}"))
        .toMat(Sink.ignore)(Keep.left)
        .run()
    }
    logger.info(s"Client: $connectionId bound to: $host:$port")

    MqttClient(session = clientSession, commands = commands)
  }

}

case class MqttClient(session: ActorMqttClientSession, commands: SourceQueueWithComplete[Command[Nothing]])

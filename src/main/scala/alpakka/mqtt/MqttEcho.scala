package alpakka.mqtt

import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.connectors.mqtt.streaming._
import org.apache.pekko.stream.connectors.mqtt.streaming.scaladsl.{ActorMqttClientSession, Mqtt}
import org.apache.pekko.stream.scaladsl.{Keep, RestartFlow, Sink, Source, SourceQueueWithComplete, Tcp}
import org.apache.pekko.stream.{OverflowStrategy, RestartSettings, ThrottleMode}
import org.apache.pekko.util.ByteString
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.util.{Failure, Success, Try}

/**
  * MQTT 3.1.1 streaming echo flow
  * See also: [[MqttPahoEcho]]
  *
  * Doc:
  * https://pekko.apache.org/docs/pekko-connectors/current/mqtt-streaming.html
  *
  * Works, but still has restart-issue on lost tcp connection, see:
  * https://discuss.lightbend.com/t/alpakka-mqtt-streaming-client-does-not-complain-when-there-is-no-tcp-connection/7113/4
  *
  * Additional inspirations:
  * TODO Handling ConnAck is key to success
  * https://github.com/michalstutzmann/scala-util/tree/master/src/main/scala/com/github/mwegrz/scalautil/mqtt
  * https://github.com/ASSIST-IoT-SRIPAS/scala-mqtt-wrapper/blob/main/examples/PekkoMain.scala
  *
  * Prerequisites:
  * Start the docker MQTT broker from: /docker/docker-compose.yml
  * eg by cmd line: docker-compose up -d mosquitto
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
    implicit val ec = system.dispatcher

    val topic = "myTopic"
    val clientId = s"pub-$id"
    val pubClient = client(clientId, sys, host, port)

    // TODO Handle ConAck
    pubClient.done.onComplete {
      case Success(value) => logger.info(s"Client: $clientId stopped with: $value. Probably lost tcp connection")
      case Failure(exception) => logger.error(s"Client: $clientId has no tcp connection on startup: ", exception)
    }

    logger.info(s"Client: $clientId bound to: $host:$port")

    Source(1 to 100)
      .throttle(1, 1.second, 1, ThrottleMode.shaping)
      .wireTap(each => logger.info(s"Client: $clientId sending: $each"))
      .map {
        msg =>
          val promise = Promise[None.type]()
          // On the server each new retained message overwrites the previous one
          val publish = Publish(ControlPacketFlags.RETAIN | ControlPacketFlags.QoSAtLeastOnceDelivery, topic, ByteString(msg.toString))
          // WIP
          // https://github.com/akka/alpakka/issues/1581
          pubClient.session ! Command(publish, () => promise.complete(Try(None)))
          promise.future
      }.runWith(Sink.ignore)
  }

  def clientSubscriber(id: Int, system: ActorSystem, host: String, port: Int): Unit = {
    implicit val sys = system
    implicit val ec = system.dispatcher

    val topic = "myTopic"
    val clientId = s"sub-$id"
    val subClient = client(clientId, sys, host, port)

    // Delay the subscription to get a "last known good value" eg 6
    Thread.sleep(5000)
    val topicFilters: Seq[(String, ControlPacketFlags)] = List((topic, ControlPacketFlags.QoSAtMostOnceDelivery))
    logger.info(s"Client: $clientId send Subscribe for topic: $topic")
    subClient.commands.offer(Command(Subscribe(topicFilters)))
  }


  // Common client for Publisher/Subscriber
  private def client(clientId: String, system: ActorSystem, host: String, port: Int): MqttClient = {
    implicit val sys = system
    implicit val ec: ExecutionContextExecutor = system.dispatcher

    logger.info(s"Client: $clientId starting...")

    val settings = MqttSessionSettings()
    val clientSession = ActorMqttClientSession(settings)

    val connection = Tcp().outgoingConnection(host, port)

    val mqttFlow =
      Mqtt
        .clientSessionFlow(clientSession, ByteString(clientId))
        .join(connection)

    val restartSettings = RestartSettings(1.second, 10.seconds, 0.2).withMaxRestarts(10, 1.minute)
    val restartFlow = RestartFlow.onFailuresWithBackoff(restartSettings)(() => mqttFlow)

    val (commands, done) = {
      Source
        .queue(10, OverflowStrategy.backpressure, 10)
        .via(restartFlow)
        // Filter the Ack events
        .filter {
          case Right(Event(_: ConnAck, _)) =>
            logger.info(s"Client: $clientId received ConnAck")
            false
          case Right(Event(_: SubAck, _)) =>
            logger.info(s"Client: $clientId received SubAck")
            false
          case Right(Event(pa: PubAck, Some(carry))) =>
            logger.info(s"Client: $clientId received PubAck for: ${pa.packetId}")
            // WIP carry is of type Nothing
            // https://github.com/akka/alpakka/pull/1908
            // carry.asInstanceOf[Promise[Done]].success(Done)
            false
          case _ => true
        }

        // Only the Publish events are interesting for the subscriber
        .collect { case Right(Event(p: Publish, _)) => p }
        .wireTap(event => logger.info(s"Client: $clientId received: ${event.payload.utf8String}"))
        .toMat(Sink.ignore)(Keep.both)
        .run()
    }

    val connectCommand = Command(Connect(clientId, ConnectFlags.CleanSession))

    logger.info("About to connect...")
    commands.offer(connectCommand)

    MqttClient(session = clientSession, commands = commands, done = done)
  }
}

case class MqttClient(session: ActorMqttClientSession, commands: SourceQueueWithComplete[Command[Nothing]], done: Future[Done])

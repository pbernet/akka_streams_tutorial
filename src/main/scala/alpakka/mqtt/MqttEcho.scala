package alpakka.mqtt

import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.connectors.mqtt.streaming._
import org.apache.pekko.stream.connectors.mqtt.streaming.scaladsl.{ActorMqttClientSession, Mqtt}
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source, SourceQueueWithComplete, Tcp}
import org.apache.pekko.stream.{OverflowStrategy, ThrottleMode}
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
  * Works, with (partially) resolved restart-issue on startup as well as during operation, see:
  * https://discuss.lightbend.com/t/alpakka-mqtt-streaming-client-does-not-complain-when-there-is-no-tcp-connection/7113/4
  *
  * TODO Remaining issue: clientPublisher flow restarts during operation lead to:
  * - a new source is starting re-sending all elements (Idea: save the pointer from the last msg in atomic param)
  * - the current source continues to run (Idea: stop with kill switch)
  *
  * Additional inspirations:
  * https://github.com/michalstutzmann/scala-util/tree/master/src/main/scala/com/github/mwegrz/scalautil/mqtt
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
    val clientId = s"Pub-$id"
    val connAckPromise = Promise[Unit]

    val pubClient = client(clientId, sys, host, port, connAckPromise)

    // A received ConAck confirms that we are connected
    connAckPromise.future.onComplete { _: Try[Unit] =>
      logger.info(s"$clientId bound to: $host:$port")

      Source(1 to 100)
        .throttle(1, 1.second, 1, ThrottleMode.shaping)
        .map(each => s"$id-$each")
        .wireTap(each => logger.info(s"$clientId sending payload: $each"))
        .map {
          msg =>
            // On the server each new retained message overwrites the previous one
            val publish = Publish(ControlPacketFlags.RETAIN | ControlPacketFlags.QoSAtLeastOnceDelivery, topic, ByteString(msg.toString))
            pubClient.session ! Command(publish, None)
        }.runWith(Sink.ignore)
    }

    pubClient.done.onComplete {
      case Success(value) =>
        logger.info(s"$clientId stopped with: $value. Probably lost tcp connection. Restarting...")
        clientPublisher(id, system, host, port)
      case Failure(exception) => logger.error(s"$clientId has no tcp connection on startup. Ex: ${exception.getMessage}. Restarting...")
        Thread.sleep(1000)
        clientPublisher(id, system, host, port)
    }
  }

  def clientSubscriber(id: Int, system: ActorSystem, host: String, port: Int): Unit = {
    implicit val sys = system
    implicit val ec = system.dispatcher

    val topic = "myTopic"
    val clientId = s"Sub-$id"
    val connAckPromise = Promise[Unit]
    val subClient = client(clientId, sys, host, port, connAckPromise)

    // A received ConAck confirms that we are connected
    connAckPromise.future.onComplete { _: Try[Unit] =>
      logger.info(s"$clientId bound to: $host:$port")

      // Delay the subscription to get a "last known good value" eg 6
      Thread.sleep(5000)
      val topicFilters: Seq[(String, ControlPacketFlags)] = List((topic, ControlPacketFlags.QoSAtMostOnceDelivery))
      logger.info(s"$clientId send Subscribe for topic: $topic")
      subClient.commands.offer(Command(Subscribe(topicFilters)))
    }

    subClient.done.onComplete {
      case Success(value) =>
        logger.info(s"$clientId stopped with: $value. Probably lost tcp connection. Restarting...")
        Thread.sleep(2000)
        clientSubscriber(id, system, host, port)
      case Failure(exception) => logger.error(s"$clientId has no tcp connection on startup. Ex: ${exception.getMessage}. Restarting...")
        Thread.sleep(2000)
        clientSubscriber(id, system, host, port)
    }
  }


  // Common client for Publisher/Subscriber
  private def client(clientId: String, system: ActorSystem, host: String, port: Int, connAckPromise: Promise[Unit]): MqttClient = {
    implicit val sys = system
    implicit val ec: ExecutionContextExecutor = system.dispatcher

    logger.info(s"$clientId starting...")

    val settings = MqttSessionSettings()
    val clientSession = ActorMqttClientSession(settings)

    val connection = Tcp().outgoingConnection(host, port)

    val mqttFlow =
      Mqtt
        .clientSessionFlow(clientSession, ByteString(clientId))
        .join(connection)

    val (commands, done) = {
      Source
        .queue(10, OverflowStrategy.backpressure, 10)
        .via(mqttFlow)
        // Filter the Ack events
        .filter {
          case Right(Event(_: ConnAck, _)) =>
            logger.info(s"$clientId received ConnAck")
            connAckPromise.complete(Success(()))
            false
          case Right(Event(_: SubAck, _)) =>
            logger.info(s"$clientId received SubAck")
            false
          case Right(Event(pa: PubAck, _)) =>
            logger.info(s"$clientId received PubAck for: ${pa.packetId}")
            false
          case _ => true
        }

        // Only the Publish events are interesting for the subscriber
        .collect { case Right(Event(p: Publish, _)) => p }
        .wireTap(event => logger.info(s"$clientId received payload: ${event.payload.utf8String}"))
        .toMat(Sink.ignore)(Keep.both)
        .run()
    }

    logger.info(s"$clientId. About to send connect cmd...")
    val connectCommand = Command(Connect(clientId, ConnectFlags.CleanSession))
    commands.offer(connectCommand)

    MqttClient(session = clientSession, commands = commands, done = done)
  }
}

case class MqttClient(session: ActorMqttClientSession, commands: SourceQueueWithComplete[Command[Nothing]], done: Future[Done])

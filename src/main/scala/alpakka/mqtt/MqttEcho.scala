package alpakka.mqtt

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.alpakka.mqtt.streaming.scaladsl.{ActorMqttClientSession, Mqtt}
import akka.stream.scaladsl.{Keep, RestartFlow, Sink, Source, SourceQueueWithComplete, Tcp}
import akka.stream.{OverflowStrategy, RestartSettings, ThrottleMode}
import akka.util.ByteString
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.parallel.CollectionConverters._
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.util.{Failure, Success, Try}

/**
  * The MQTT Streaming connector. Work in progress...
  * See also: [[MqttPahoEcho]]
  *
  * Doc:
  * https://doc.akka.io/docs/alpakka/current/mqtt-streaming.html
  *
  * Inspiration:
  * https://github.com/michalstutzmann/scala-util/tree/master/src/main/scala/com/github/mwegrz/scalautil/mqtt
  *
  * Prerequisite:
  * Start the docker MQTT broker from: /docker/docker-compose.yml
  * eg by cmd line: docker-compose up -d mosquitto
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


    val connectCommand = Command(Connect(clientId, ConnectFlags.CleanSession))
    if (! pub.done.isCompleted) pub.commands.offer(connectCommand)


    Source(1 to 100)
      .throttle(1, 1.second, 1, ThrottleMode.shaping)
      .wireTap(each => logger.info(s"Client sending: $each"))
      //TODO With mapAsync this is hanging after the 1st msg - do the results need to be consumed/pulled?
      //It looks as if the ConnAck/PubAck/ need to be handled to see if we are still connected...
      .map {
        msg =>

          //if (pub.done.isCompleted) throw new RuntimeException("Server not reachable")

          //TODO What is the benefit of this meccano? Does the Promise carry a ACK Result?
          val promise = Promise[None.type]()
          //On the server each new retained message overwrites the previous one
          val publish = Publish(ControlPacketFlags.RETAIN | PublishQoSFlags.QoSAtLeastOnceDelivery, topic, ByteString(msg.toString))
          pub.session ! Command(publish, () => promise.complete(Try(None)))
          promise.future
      }
      .runWith(Sink.ignore)
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
    val topicFilters: Seq[(String, ControlPacketFlags)] = List((topic, PublishQoSFlags.QoSAtMostOnceDelivery))
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

    val mqttFlow =
      Mqtt
        .clientSessionFlow(clientSession, ByteString(connectionId))
        .join(connection)

    val restartSettings = RestartSettings(1.second, 10.seconds, 0.2).withMaxRestarts(10, 1.minute)
    val restartFlow = RestartFlow.onFailuresWithBackoff(restartSettings)(() => mqttFlow)

    val (commands, done) = {
      Source
        .queue(10, OverflowStrategy.backpressure, 10)
        .via(restartFlow)

        //TODO Process ConnAck/SubAck/PubAck as an indicator on the application level to see if we are still connected
        //Coordinate via additional ConnectedActor?
        //Additional hints:
        //https://github.com/akka/alpakka/issues/1581

        .filter {
          case Right(Event(_: ConnAck, _)) =>
            logger.info("Received ConnAck")
            false
          case Right(Event(_: SubAck, _)) =>
            logger.info("Received SubAck")
            false
          case Right(Event(p: PubAck, Some(ack))) =>
            logger.info(s"Received PubAck" + p.packetId)
            false
          case _ => true
        }


        //Only the Publish events are interesting for the subscriber
        .collect { case Right(Event(p: Publish, _)) => p }
        .wireTap(event => logger.info(s"Client: $connectionId received: ${event.payload.utf8String}"))
        .toMat(Sink.ignore)(Keep.both)
        .run()
    }


    //TODO https://discuss.lightbend.com/t/alpakka-mqtt-streaming-client-does-not-complain-when-there-is-no-tcp-connection/7113
    done.onComplete{
      case Success(value) => logger.info(s"Flow stopped with: $value. Probably lost tcp connection")
      case Failure(exception) => logger.error("Error no tcp connection on startup:", exception)
    }

    //WIP: Due to the async nature of the flow above, we don't know if we are really connected
    logger.info(s"Client: $connectionId bound to: $host:$port")
    MqttClient(session = clientSession, commands = commands, done = done)
  }
}

case class MqttClient(session: ActorMqttClientSession, commands: SourceQueueWithComplete[Command[Nothing]], done: Future[Done])

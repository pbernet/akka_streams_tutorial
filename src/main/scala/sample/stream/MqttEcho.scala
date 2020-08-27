package sample.stream

import akka.actor.ActorSystem
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.alpakka.mqtt.streaming.scaladsl.{ActorMqttClientSession, ActorMqttServerSession, Mqtt}
import akka.stream.scaladsl.{BroadcastHub, Flow, Keep, Sink, Source, SourceQueueWithComplete, Tcp}
import akka.stream.{KillSwitches, OverflowStrategy, UniqueKillSwitch}
import akka.util.ByteString
import akka.{Done, NotUsed}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.util.{Failure, Success}

/**
  * Inspired by:
  * https://doc.akka.io/docs/alpakka/current/mqtt-streaming.html
  *
  * Use without parameters to start server and 10 parallel clients.
  *
  * Use parameters `server 0.0.0.0 1883` to start server listening on port 1883
  *
  * Use parameters `client 127.0.0.1 1883` to start client connecting to
  * server on 127.0.0.1:6001
  *
  */
object MqttEcho extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val systemServer = ActorSystem("MqttEchoServer")
  val systemClient = ActorSystem("MqttEchoClient")

  var serverBinding: Future[Tcp.ServerBinding] = _

  if (args.isEmpty) {
    val (host, port) = ("127.0.0.1", 1883)
    serverBinding = server(systemServer, host, port)
    (1 to 1).par.foreach(each => clientSubscriber(each, systemClient, host, port))
    (1 to 1).par.foreach(each => clientPublisher(each, systemClient, host, port))

  } else {
    val (host, port) =
      if (args.length == 3) (args(1), args(2).toInt)
      else ("127.0.0.1", 1883)
    if (args(0) == "server") {
      serverBinding = server(systemServer, host, port)
    } else if (args(0) == "client") {
      clientPublisher(1, systemClient, host, port)
    }
  }

  def server(system: ActorSystem, host: String, port: Int): Future[Tcp.ServerBinding] = {
    implicit val sys = system
    implicit val ec = system.dispatcher

    val settings = MqttSessionSettings()
    val session = ActorMqttServerSession(settings)

    //TODO If set to value > 1, then two connect cmds arrive
    val maxConnections = 100

    val bindSource: Source[Either[MqttCodec.DecodeError, Event[Nothing]], Future[Tcp.ServerBinding]] =
      Tcp()
        .bind(host, port)
        .flatMapMerge(
          maxConnections, { connection =>
            val mqttFlow: Flow[Command[Nothing], Either[MqttCodec.DecodeError, Event[Nothing]], NotUsed] =
              Mqtt
                .serverSessionFlow(session, ByteString(connection.remoteAddress.getAddress.getAddress))
                .join(connection.flow)

            val (queue, source) = Source
              .queue[Command[Nothing]](3, OverflowStrategy.dropHead)
              .via(mqttFlow)
              .toMat(BroadcastHub.sink)(Keep.both)
              .run()

            val subscribed = Promise[Done]
            source
              .runForeach {
                case Right(connect@Event(_: Connect, _)) =>
                  logger.info(s"Server received command: $connect")
                  queue.offer(Command(ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)))
                case Right(subscribe@Event(cp: Subscribe, _)) =>
                  logger.info(s"Server received command: $subscribe")
                  queue.offer(Command(SubAck(cp.packetId, cp.topicFilters.map(_._2)), Some(subscribed), None))
                case Right(Event(publish@Publish(flags, _, Some(packetId), _), _))
                  if flags.contains(ControlPacketFlags.RETAIN) =>
                  logger.info(s"Server received command: $publish")
                  queue.offer(Command(PubAck(packetId)))
                  subscribed.future.foreach(_ => session ! Command(publish))
                case _ => // Ignore everything else
              }

            source
          }
        )

    val (bound: Future[Tcp.ServerBinding], server: UniqueKillSwitch) = bindSource
      .viaMat(KillSwitches.single)(Keep.both)
      .to(Sink.ignore)
      .run()

    //    val binding = bound.futureValue
    //    binding.localAddress.getPort should not be 0
    //    server.shutdown()

    bound.onComplete {
      case Success(b) =>
        logger.info("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        logger.info(s"Server could not bind to: $host:$port: ${e.getMessage}")
        system.terminate()
    }
    bound
  }


  def clientPublisher(id: Int, system: ActorSystem, host: String, port: Int): Unit = {
    implicit val sys = system
    implicit val ec: ExecutionContextExecutor = system.dispatcher

    val topic = "myTopic"
    val clientId = s"$id-pub"
    val pub = client(clientId, system, host, port)

    pub.commands.offer(Command(Connect(clientId, ConnectFlags.None)))
    //TODO Publish to my own cmds works, but the clientSubscriber does not get this message
    pub.commands.offer(Command(Subscribe(topic)))

    //Thread.sleep(10000)

    //The Publish command is not offered to the command flow given MQTT QoS requirements.
    //Instead, the session is told to perform Publish given that it can retry continuously with buffering until a command flow is established.
    pub.session ! Command(
      Publish(ControlPacketFlags.RETAIN | ControlPacketFlags.QoSAtLeastOnceDelivery, topic, ByteString("payload1"))
    )

    //TODO Does not seem to have an effect due to "last known good value" concept
    pub.session ! Command(
      Publish(ControlPacketFlags.RETAIN | ControlPacketFlags.QoSAtLeastOnceDelivery, topic, ByteString("payload2"))
    )

    //Events: ACKs to our connect, subscribe and publish. ?? The collected event is the publication to the topic we just subscribed to.
    pub.events.foreach(each => logger.info(s"Client with id: $clientId received: $each"))
  }

  def clientSubscriber(id: Int, system: ActorSystem, host: String, port: Int): Unit = {
    implicit val sys = system
    implicit val ec: ExecutionContextExecutor = system.dispatcher

    val topic = "myTopic"
    val clientId = s"$id-sub"
    val sub = client(clientId, system, host, port)

    sub.commands.offer(Command(Connect(clientId, ConnectFlags.None)))
    sub.commands.offer(Command(Subscribe(topic)))

    //Events: ACKs to our connect, subscribe and publish. ?? The collected event is the publication to the topic we just subscribed to.
    sub.events.foreach(each => logger.info(s"Client with id: $clientId received: $each"))
  }





  private def client(connectionId: String, system: ActorSystem, host: String, port: Int): MqttClient = {
    implicit val sys = system
    implicit val ec: ExecutionContextExecutor = system.dispatcher

    val settings = MqttSessionSettings()
    val clientSession = ActorMqttClientSession(settings)

    val connection = Tcp().outgoingConnection(host, port)

    val mqttFlow: Flow[Command[Nothing], Either[MqttCodec.DecodeError, Event[Nothing]], NotUsed] =
      Mqtt
        .clientSessionFlow(clientSession, ByteString(connectionId))
        .join(connection)

    val (commands, events) = {
      Source
        .queue(2, OverflowStrategy.fail)
        .via(mqttFlow)
        .collect {
          case Right(Event(p: Publish, _)) => p
        }
        .toMat(Sink.head)(Keep.both)
        .run()
    }
    logger.info(s"Client with id: $connectionId bound to: $host:$port")

    MqttClient(session = clientSession, commands = commands, events = events)
  }

}

case class MqttClient(session: ActorMqttClientSession, commands: SourceQueueWithComplete[Command[Nothing]], events: Future[Publish])

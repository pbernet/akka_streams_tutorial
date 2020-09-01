package sample.stream

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.alpakka.mqtt.streaming.scaladsl.{ActorMqttClientSession, ActorMqttServerSession, Mqtt}
import akka.stream.scaladsl.{BroadcastHub, Keep, Sink, Source, SourceQueueWithComplete, Tcp}
import akka.stream.{KillSwitches, OverflowStrategy, ThrottleMode, UniqueKillSwitch}
import akka.util.ByteString
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.util.{Failure, Success}

/**
  * Inspired by:
  * https://doc.akka.io/docs/alpakka/current/mqtt-streaming.html
  *
  * Use without parameters to start server and 10 parallel clients
  *
  * Use parameter `server` to start internal mock server listening on 127.0.0.1:1883
  *
  * Use parameter `client` to start n publisher/subscriber clients connecting to server on 127.0.0.1:1883
  *
  */
object MqttEcho extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val systemServer = ActorSystem("MqttEchoServer")
  val systemClient1 = ActorSystem("MqttEchoClient1")
  val systemClient2 = ActorSystem("MqttEchoClient2")

  var serverBinding: Future[Tcp.ServerBinding] = _

  if (args.isEmpty) {
    val (host, port) = ("127.0.0.1", 1883)
    //TODO Why is client not complaining, when server is not reachable?
    serverBinding = server(systemServer, host, port)
    //TODO WHY is clientSubscriber running in the same thread as clientPublisher?
    //Why no parallelism? Is it because the actor system is passed down?
    //Try with only one ActorSystem for all
    (1 to 1).par.foreach(each => clientPublisher(each, systemClient1, host, port))
    (1 to 2).par.foreach(each => clientSubscriber(each, systemClient2, host, port))


  } else {
    val (host, port) =
      if (args.length == 3) (args(1), args(2).toInt)
      else ("127.0.0.1", 1883)
    if (args(0) == "server") {
      serverBinding = server(systemServer, host, port)
    } else if (args(0) == "client") {
      (1 to 1).par.foreach(each => clientPublisher(each, systemClient1, host, port))
      (1 to 2).par.foreach(each => clientSubscriber(each, systemClient2, host, port))
    }
  }

  /**
    * Provides a MQTT server flow in the case where you do not wish to use an external
    * MQTT broker such as HiveMQ/eclipse-mosquitto
    *
    * Use for directed client/server interactions
    *
    */
  def server(system: ActorSystem, host: String, port: Int): Future[Tcp.ServerBinding] = {
    implicit val sys = system
    implicit val ec = system.dispatcher

    val settings = MqttSessionSettings()
    val session = ActorMqttServerSession(settings)

    val maxConnections = 100

    val bindSource: Source[Either[MqttCodec.DecodeError, Event[Nothing]], Future[Tcp.ServerBinding]] =
      Tcp()
        .bind(host, port)
        .flatMapMerge(
          maxConnections, { connection =>
            val mqttFlow =
              Mqtt
                .serverSessionFlow(session, ByteString(connection.remoteAddress.getAddress.getAddress))
                .join(connection.flow)

            val (replyCmdQueue, receivedEventSource) = Source
              .queue[Command[Nothing]](10, OverflowStrategy.dropHead)
              .via(mqttFlow)
              .toMat(BroadcastHub.sink)(Keep.both)
              .run()

            val subscribed = Promise[Done]
            receivedEventSource
              .runForeach {
                case Right(Event(cp: Connect, _)) =>
                  logger.info(s"Server received Connect from client: ${cp.clientId}")
                  replyCmdQueue.offer(Command(ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)))
                case Right(Event(cp: Subscribe, _)) =>
                  //TODO Can I see which client id has subscribed for which topic?
                  //val topics =  cp.topicFilters.collect(each => each._2)
                  logger.info(s"Server received Subscribe for topic(s): ${cp.topicFilters}")
                  replyCmdQueue.offer(Command(SubAck(cp.packetId, cp.topicFilters.map(_._2)), Some(subscribed), None))
                case Right(Event(publish@Publish(flags, topicName, Some(packetId), payload), _))
                  if flags.contains(ControlPacketFlags.RETAIN) =>

                  logger.info(s"Server received Publish for topic: $topicName with payload: ${payload.utf8String}")
                  replyCmdQueue.offer(Command(PubAck(packetId)))

                  //hold off re-publishing until we have a subscription from a client
                  subscribed.future.foreach { _ =>
                    logger.info(s"Server distribute: ${payload.utf8String} to subscribed clients")
                    session ! Command(publish)
                  }

                case otherEvent@Right(Event(_, _)) => logger.info(s"Server received Event: $otherEvent") // Ignore everything else
              }
            receivedEventSource
          }
        )

    val (bound: Future[Tcp.ServerBinding], server: UniqueKillSwitch) = bindSource
      .viaMat(KillSwitches.single)(Keep.both)
      .to(Sink.ignore)
      .run()

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
        val publish = Publish(ControlPacketFlags.RETAIN | ControlPacketFlags.QoSAtMostOnceDelivery, topic, ByteString(each.toString))
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

    //Wait with the Subscribe to get the "last known good value" eg 6
    Thread.sleep(5000)
    val topicFilters: Seq[(String, ControlPacketFlags)] = List((topic, ControlPacketFlags.QoSAtMostOnceDelivery))
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

    val commands = {
      Source
        .queue(10, OverflowStrategy.fail)
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

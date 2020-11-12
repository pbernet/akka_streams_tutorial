package alpakka.mqtt

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.alpakka.mqtt.streaming.scaladsl.{ActorMqttClientSession, Mqtt}
import akka.stream.scaladsl.{Keep, RestartFlow, Sink, Source, SourceQueueWithComplete, Tcp}
import akka.stream.{OverflowStrategy, ThrottleMode}
import akka.util.ByteString
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.parallel.CollectionConverters._
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future, Promise}
import scala.util.{Failure, Success, Try}

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


    val connectCommand = Command(Connect(clientId, ConnectFlags.CleanSession))
    if (! pub.done.isCompleted) pub.commands.offer(connectCommand)


    Source(1 to 100)
      .throttle(elements = 1, per = 1.second, maximumBurst = 1, mode = ThrottleMode.shaping)
      .wireTap(each => logger.info(s"Client sending: $each"))
      //TODO With mapAsync this is hanging after the 1st msg - do the results need to be consumed/pulled?
      //It looks as if the ConnAck/PubAck/ need to be handled to see if we are still connected...
      .map {
        msg =>

          //if (pub.done.isCompleted) throw new RuntimeException("Server not reachable")

          //TODO What is the benefit of this meccano? Does the Promise carry a ACK Result?
          val promise = Promise[None.type]()
          //On the server each new retained message overwrites the previous one
          val publish = Publish(ControlPacketFlags.RETAIN | ControlPacketFlags.QoSAtLeastOnceDelivery, topic, ByteString(msg.toString))
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

    val mqttFlow =
      Mqtt
        .clientSessionFlow(clientSession, ByteString(connectionId))
        .join(connection)

    val restartFlow = RestartFlow.onFailuresWithBackoff(1.second, 10.seconds, 0.2, 10)(() => mqttFlow)

    val (commands, done) = {
      Source
        .queue(10, OverflowStrategy.backpressure,10)
        .via(restartFlow)

        //TODO Read ConnAck/SubAck/PubAck and send to ConnectedActor to be able to see if we are still connected
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


        //Only the Publish events are interesting
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



//  //TODO inspiration
//  //https://github.com/michalstutzmann/scala-util/tree/master/src/main/scala/com/github/mwegrz/scalautil/mqtt
//
//  override def createFlow[A, B](topics: Map[String, Qos], qos: Qos, name: String)(implicit
//                                                                                  aSerde: Serde[A],
//                                                                                  bSerde: Serde[B]
//  ): Flow[(String, A), (String, B), Future[Connected]] = {
//    val connection = Tcp().outgoingConnection(host, port)
//    val session = ActorMqttClientSession(MqttSessionSettings())
//    val uuid = UUID.randomUUID()
//    val clientSessionFlow: Flow[Command[() => Unit], Either[MqttCodec.DecodeError, Event[() => Unit]], Future[
//      Tcp.OutgoingConnection
//    ]] =
//      Mqtt
//        .clientSessionFlow(session, ByteString(uuid.toString))
//        .joinMat(connection)(Keep.right)
//    val connectCommand = Connect(
//      if (clientId.isEmpty) uuid.toString else clientId,
//      ConnectFlags.CleanSession,
//      username,
//      password
//    )
//    val connAckPromise = Promise[Unit]
//    val subAckPromise = if (topics.nonEmpty) Promise[Unit] else Promise.successful(())
//
//    Flow[(String, A)]
//      .mapAsync(parallelism) {
//        case (topic, msg) =>
//          val promise = Promise[None.type]()
//          session ! Command(
//            Publish(qos.toControlPacketFlags, topic, ByteString(aSerde.valueToBytes(msg).toArray)),
//            () => promise.complete(Try(None))
//          )
//          promise.future
//      }
//      .mapConcat(_ => Nil)
//      //Interesting concept prepend
//      .prepend(
//        Source(
//          if (topics.nonEmpty) {
//            Iterable(
//              Command[() => Unit](connectCommand),
//              Command[() => Unit](
//                Subscribe(topics.map { case (name, qos) => (name, qos.toControlPacketFlags) }.toSeq)
//              )
//            )
//          } else {
//            Iterable(Command[() => Unit](connectCommand))
//          }
//        )
//      )
//      .viaMat(clientSessionFlow)(Keep.right)
//      .filter {
//        case Right(Event(_: ConnAck, _)) =>
//          connAckPromise.complete(Success(()))
//          false
//        case Right(Event(_: SubAck, _)) if topics.nonEmpty =>
//          subAckPromise.complete(Success(()))
//          false
//        case Right(Event(_: PubAck, Some(ack))) =>
//          ack()
//          false
//        case _ => true
//      }
//      .collect {
//        case Right(Event(p: Publish, _)) =>
//          (p.topicName, bSerde.bytesToValue(ByteVector(p.payload.toArray)))
//      }
//      .watchTermination() { (outgoingConnection, f) =>
//        f.onComplete {
//          case Success(_) =>
//            session.shutdown()
//            log.debug("Flow completed", "name" -> name)
//          case Failure(exception) =>
//            session.shutdown()
//            throw log.error("Flow failed", exception, "name" -> name)
//        }
//
//        log.debug("Flow created", "name" -> name)
//
//        val connected = for {
//          value <- outgoingConnection
//          _ <- connAckPromise.future
//          _ <- subAckPromise.future
//        } yield {
//          Connected(value)
//        }
//
//        connected.onComplete {
//          case Success(Connected(value)) =>
//            log.debug(
//              "Connection established",
//              ("name" -> name, "host" -> value.remoteAddress.getHostName, "port" -> value.remoteAddress.getPort)
//            )
//          case Failure(exception) =>
//            log.error("Connection failed", exception, "name" -> name)
//        }
//
//        connected
//      }
//  }




}

case class MqttClient(session: ActorMqttClientSession, commands: SourceQueueWithComplete[Command[Nothing]], done: Future[Done])

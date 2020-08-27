package sample.stream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.mqtt.streaming.scaladsl.{ActorMqttServerSession, Mqtt}
import akka.stream.alpakka.mqtt.streaming._
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source, Tcp, Unzip}
import akka.stream.{KillSwitches, OverflowStrategy, SourceShape}
import akka.util.ByteString

import scala.concurrent.{ExecutionContext, Future}

/**
  * Tmp stolen from:
  * https://discuss.lightbend.com/t/mqtt-only-publishers/6166
  *
  * @param actorSystem
  * @param ec
  */
class MqttServer(
                )(
                  implicit actorSystem: ActorSystem,
                  ec: ExecutionContext
                ) {
  val port              = 1883
  val maxConnections    = 500
  val commandBufferSize = 3

  val settings = MqttSessionSettings()
  val session  = ActorMqttServerSession(settings)

  private def handler(s: Either[MqttCodec.DecodeError, Event[Nothing]]): (List[Command[Nothing]], List[Publish]) = {
    s match {
      case Right(Event(c: Connect, _)) =>
        (Command(ConnAck(ConnAckFlags.None, ConnAckReturnCode.ConnectionAccepted)) :: Nil, Nil)
      case Right(Event(cp: Subscribe, _)) =>
        (Command(SubAck(cp.packetId, cp.topicFilters.map(_._2)), None, None) :: Nil, Nil)
      case Right(Event(publish@Publish(flags, _, Some(packetId), _), _))
        if flags.contains(ControlPacketFlags.RETAIN) =>
        //println(s"pub1: ${publish.topicName} - ${publish.payload.utf8String} ")
        (Command(PubAck(packetId)) :: Nil, publish :: Nil)
      case Right(Event(publish@Publish(flags, _, _, _), _)) =>
        //println(s"pub2: ${publish.topicName} - ${publish.payload.utf8String} ")
        (Nil, publish :: Nil)
      case _ =>
        (Nil, Nil)
    }
  }

  private val bindSource: Source[Publish, Future[Tcp.ServerBinding]] =
    Tcp()
      .bind("0.0.0.0", port)
      .flatMapMerge(
        maxConnections, { connection =>
          /*
                 |-> mqttFlow ---> handler ----> pub
                 |                  |
                 |------- Command --
           */

          GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
            import GraphDSL.Implicits._

            val f = builder.add{
              val mqttFlow: Flow[Command[Nothing], Either[MqttCodec.DecodeError, Event[Nothing]], NotUsed] =
                Mqtt
                  .serverSessionFlow(session, ByteString(connection.remoteAddress.getAddress.getAddress))
                  .join(connection.flow)
              mqttFlow.map(handler)
            }

            val unzip = builder.add(Unzip[List[Command[Nothing]], List[Publish]]())
            val mcc0 = builder.add(Flow[List[Command[Nothing]]].mapConcat(identity))
            val mcc1 = builder.add(Flow[List[Publish]].mapConcat(identity))
            val buffer = builder.add(Flow[Command[Nothing]].buffer(commandBufferSize, OverflowStrategy.dropHead))

            f.out ~> unzip.in
            unzip.out0 ~> mcc0 ~> buffer ~> f
            unzip.out1 ~> mcc1

            SourceShape(mcc1.out)
          }
        }
      )

  def start() = {
    bindSource
      .viaMat(KillSwitches.single)(Keep.both)
      .to(Sink.ignore) //todo published-msg processing sink
      .run()
  }
}
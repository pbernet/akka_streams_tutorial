package alpakka.tcp_to_websockets.hl7mllp

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source, Tcp}
import akka.util.ByteString
import ca.uhn.hl7v2.AcknowledgmentCode
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future
import scala.concurrent.duration._

object Hl7TcpClient  extends App with MllpProtocol {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val system = ActorSystem("Hl7TcpClient")

  val (address, port) = ("127.0.0.1", 6160)

  //(1 to 1).par.foreach(each => localStreamingMessageClient(each, 1000, system, address, port))
  (1 to 1).par.foreach(each => localSingleMessageClient(each, 100, system, address, port))


  def localSingleMessageClient(clientname: Int, numberOfMessages: Int, system: ActorSystem, address: String, port: Int): Unit = {
    implicit val sys = system
    implicit val ec = system.dispatcher

    val connection = Tcp().outgoingConnection(address, port)

    def sendAndReceive(i: Int): Future[Int] = {
      val traceID = s"$clientname-${i.toString}"
      val source = Source.single(ByteString(encodeMllp(generateTestMessage(traceID)))).via(connection)
      val closed = source.runForeach(each =>
        if (isNACK(each)) {
          logger.info(s"Client: $clientname-$i received NACK: ${printable(each.utf8String)}")
          throw new RuntimeException("NACK")
        } else {
          logger.info(s"Client: $clientname-$i received ACK: ${printable(each.utf8String)}")
        }
      ).recoverWith {
        case _: RuntimeException => {
          logger.info(s"About to retry for: $clientname-$i...")
          sendAndReceive(i)
        }
        case e: Throwable => Future.failed(e)
      }
      closed.onComplete(each => logger.debug(s"Client: $clientname-$i closed: $each"))
      Future(i)
    }

    Source(1 to numberOfMessages)
      .throttle(1, 1.second)
      .mapAsync(1)(i => sendAndReceive(i))
      .runWith(Sink.ignore)
  }

  def localStreamingMessageClient(id: Int, numberOfMesssages: Int, system: ActorSystem, address: String, port: Int): Unit = {
    implicit val sys = system
    implicit val ec = system.dispatcher

    val connection = Tcp().outgoingConnection(address, port)

    val hl7MllpMessages=  (1 to numberOfMesssages).map(each => ByteString(encodeMllp(generateTestMessage(each.toString)) ))
    val source = Source(hl7MllpMessages).throttle(10, 1.second).via(connection)
    val closed = source.runForeach(each => logger.info(s"Client: $id received echo: ${printable(each.utf8String)}"))
    closed.onComplete(each => logger.info(s"Client: $id closed: $each"))
  }

  private def generateTestMessage(senderTraceID: String) = {
    //For now put the senderTraceID into the "sender lab" field to follow the messages accross the workflow
    val message = new StringBuilder
    message ++= s"MSH|^~\\&|$senderTraceID|MCM|LABADT|MCM|198808181126|SECURITY|ADT^A01|1234|P|2.5.1|"
    message ++= CARRIAGE_RETURN
    message ++= "EVN|A01|198808181123||"
    message ++= CARRIAGE_RETURN
    message ++= "PID|||PATID1234^5^M11^ADT1^MR^MCM~123456789^^^USSSA^SS||EVERYMAN^ADAM^A^III||19610615|M||C|1200 N ELM STREET^^GREENSBORO^NC^27401-1020"
    message ++= CARRIAGE_RETURN
    message ++= "NK1|1|JONES^BARBARA^K|SPO^Spouse^HL70063|171 ZOBERLEIN^^ISHPEMING^MI^49849^|"
    message ++= CARRIAGE_RETURN
    message ++= "PV1|1|I|2000^2012^01||||004777^LEBAUER^SIDNEY^J.|||SUR||||9|A0|"
    message ++= CARRIAGE_RETURN
    message.toString()
  }

  private def isNACK(message: ByteString): Boolean = {
    message.utf8String.contains(AcknowledgmentCode.AE.name()) ||
      message.utf8String.contains(AcknowledgmentCode.AR.name()) ||
      message.utf8String.contains(AcknowledgmentCode.CE.name()) ||
      message.utf8String.contains(AcknowledgmentCode.CR.name())
  }
}

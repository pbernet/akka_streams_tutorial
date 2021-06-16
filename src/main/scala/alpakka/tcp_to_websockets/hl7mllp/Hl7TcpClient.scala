package alpakka.tcp_to_websockets.hl7mllp

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source, Tcp}
import akka.util.ByteString
import ca.uhn.hl7v2.AcknowledgmentCode
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.parallel.CollectionConverters._
import scala.concurrent.Future
import scala.concurrent.duration._

class Hl7TcpClient(numberOfMessages: Int = 100) extends MllpProtocol {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("Hl7TcpClient")
  implicit val executionContext = system.dispatcher

  val (address, port) = ("127.0.0.1", 6160)
  val connection = Tcp().outgoingConnection(address, port)

  (1 to 1).par.foreach(each => localSingleMessageClient(each, numberOfMessages))
  //(1 to 1).par.foreach(each => localStreamingMessageClient(each, 1000))

  def localSingleMessageClient(client: Int, numberOfMessages: Int): Unit = {
    Source(1 to numberOfMessages)
      .throttle(1, 1.second)
      .mapAsync(1)(msgID => sendAndReceive(s"$client-$msgID", None))
      .runWith(Sink.ignore)
  }

  def localStreamingMessageClient(id: Int, numberOfMesssages: Int): Unit = {
    val hl7MllpMessages=  (1 to numberOfMesssages).map(each => ByteString(encodeMllp(generateTestMessage(each.toString)) ))
    val source = Source(hl7MllpMessages).throttle(10, 1.second).via(connection)
    val closed = source.runForeach(each => logger.info(s"Client: $id received echo: ${printable(each.utf8String)}"))
    closed.onComplete(each => logger.info(s"Client: $id closed: $each"))
  }

  private def sendAndReceive(traceID: String, hl7Msg: Option[String]): Future[NotUsed] = {
    val hl7MsgString = hl7Msg.getOrElse(generateTestMessage(traceID))
    val source = Source.single(ByteString(encodeMllp(hl7MsgString))).via(connection)
    val closed = source.runForeach(each =>
      if (isNACK(each)) {
        logger.info(s"Client for traceID: $traceID received NACK: ${printable(each.utf8String)}")
        throw new RuntimeException("NACK")
      } else {
        logger.info(s"Client for traceID: $traceID received ACK: ${printable(each.utf8String)}")
      }
    ).recoverWith {
      case ex: RuntimeException =>
        logger.warn(s"Client for traceID: $traceID about to retry, because of: $ex")
        sendAndReceive(traceID, hl7Msg)
      case e: Throwable => Future.failed(e)
    }
    closed.onComplete(each => logger.debug(s"Client for traceID: $traceID closed: $each"))
    Future(NotUsed)
  }


  private def generateTestMessage(senderTraceID: String) = {
    //For now put the senderTraceID into the "sender lab" field to follow the messages across the workflow
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

object Hl7TcpClient extends App {
  val client = new Hl7TcpClient()
  def apply(numberOfMessages: Int = 100) = new Hl7TcpClient(numberOfMessages)
}

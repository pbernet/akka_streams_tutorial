package alpakka.tcp_to_websockets.hl7mllp

import ca.uhn.hl7v2.AcknowledgmentCode
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source, Tcp}
import org.apache.pekko.util.ByteString
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.parallel.CollectionConverters.ImmutableIterableIsParallelizable
import scala.concurrent.Future
import scala.concurrent.duration.*

/**
  * Client to send HL7 msgs to [[Hl7Tcp2Kafka]]
  */
class Hl7TcpClient(numberOfMessages: Int = 100) extends MllpProtocol {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val (address, port) = ("127.0.0.1", 6160)
  val connection: Flow[ByteString, ByteString, Future[Tcp.OutgoingConnection]] = Tcp().outgoingConnection(address, port)

  (1 to 2).par.foreach(each => localSingleMessageClient(each, numberOfMessages))

  def localSingleMessageClient(client: Int, nbrOfMgs: Int): Unit = {
    Source(1 to nbrOfMgs)
      .throttle(1, 1.second)
      .mapAsync(1)(msgID => sendAndReceive(s"$client-$msgID"))
      .runWith(Sink.ignore)
  }

  def localStreamingMessageClient(id: Int, nbrOfMgs: Int): Unit = {
    val messages = (1 to nbrOfMgs).map(each => ByteString(encodeMllp(generateTestMessage(each.toString))))
    val closed = Source(messages)
      .throttle(10, 1.second)
      .via(connection)
      .runForeach(each => logger.info(s"Client: $id received echo: ${printable(each.utf8String)}"))
    closed.onComplete(each => logger.info(s"Client: $id closed: $each"))
  }

  private def sendAndReceive(traceID: String): Future[NotUsed] = {
    val message = ByteString(encodeMllp(generateTestMessage(traceID)))
    val closed = Source.single(message).via(connection).runForeach { response =>
      if (isNACK(response)) {
        logger.info(s"Client for traceID: $traceID received NACK: ${printable(response.utf8String)}")
        throw new RuntimeException("NACK")
      } else {
        logger.info(s"Client for traceID: $traceID received ACK: ${printable(response.utf8String)}")
      }
    }.recoverWith {
      case ex: RuntimeException =>
        logger.warn(s"Client for traceID: $traceID about to retry, because of: $ex")
        sendAndReceive(traceID)
    }
    closed.onComplete(each => logger.debug(s"Client for traceID: $traceID closed: $each"))
    Future(NotUsed)
  }

  private def generateTestMessage(senderTraceID: String): String = {
    // For now put the senderTraceID into the "sender lab" field to follow the messages across the workflow
    Seq(
      s"MSH|^~\\&|$senderTraceID|MCM|LABADT|MCM|198808181126|SECURITY|ADT^A01|1234|P|2.5.1|",
      "EVN|A01|198808181123||",
      "PID|||PATID1234^5^M11^ADT1^MR^MCM~123456789^^^USSSA^SS||EVERYMAN^ADAM^A^III||19610615|M||C|1200 N ELM STREET^^GREENSBORO^NC^27401-1020",
      "NK1|1|JONES^BARBARA^K|SPO^Spouse^HL70063|171 ZOBERLEIN^^ISHPEMING^MI^49849^|",
      "PV1|1|I|2000^2012^01||||004777^LEBAUER^SIDNEY^J.|||SUR||||9|A0|")
      .mkString("", CARRIAGE_RETURN, CARRIAGE_RETURN)
  }

  private def isNACK(message: ByteString): Boolean = {
    val response = message.utf8String
    Seq(AcknowledgmentCode.AE, AcknowledgmentCode.AR, AcknowledgmentCode.CE, AcknowledgmentCode.CR)
      .exists(code => response.contains(code.name()))
  }
}

object Hl7TcpClient extends App {
  val client = new Hl7TcpClient()

  def apply(numberOfMessages: Int = 100): Hl7TcpClient = new Hl7TcpClient(numberOfMessages)
}

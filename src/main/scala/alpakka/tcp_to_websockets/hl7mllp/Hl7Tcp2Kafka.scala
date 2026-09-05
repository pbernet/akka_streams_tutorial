package alpakka.tcp_to_websockets.hl7mllp

import ca.uhn.hl7v2.validation.impl.ValidationContextFactory
import ca.uhn.hl7v2.{AcknowledgmentCode, DefaultHapiContext, HL7Exception}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.kafka.ProducerSettings
import org.apache.pekko.kafka.scaladsl.SendProducer
import org.apache.pekko.stream.scaladsl.{Flow, Framing, Keep, Sink, Tcp}
import org.apache.pekko.stream.{ActorAttributes, Supervision}
import org.apache.pekko.util.ByteString
import org.slf4j.{Logger, LoggerFactory}

import scala.compiletime.uninitialized
import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/**
  * PoC of a pekko streams based HL7 MLLP listener with the same behavior as [[Hl7MllpListener]]:
  *  - Receive HL7 messages over tcp
  *  - Frame according to MLLP protocol
  *  - Parse using HAPI parser (all validation switched off)
  *  - Send to Kafka topic
  *  - Reply to client with ACK or NACK (e.g. on connection issues)
  *
  * Run Kafka e.g. [[KafkaServerEmbedded]]
  * Run this class to start the server
  * Run [[Hl7TcpClient]] to produce messages
  *
  */
class Hl7Tcp2Kafka(mappedPortKafka: Int = 29092) extends MllpProtocol {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val bootstrapServers = s"127.0.0.1:$mappedPortKafka"
  val topic = "hl7-input"
  // initial msg in topic, required to create the topic before any consumer subscribes to it
  val InitialMsg = "InitialMsg"
  val partition0 = 0
  val producerSettings: ProducerSettings[String, String] = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)
  val producer: SendProducer[String, String] = SendProducer(producerSettings)
  initializeTopic(topic).failed.foreach(error => logger.error(s"Failed to initialize Kafka topic $topic", error))

  val (address, port) = ("127.0.0.1", 6160)
  var serverBinding: Future[Tcp.ServerBinding] = uninitialized

  def run(): Unit = {
    serverBinding = server(address, port)
    logger.info(s"Sending messages to Kafka on: $bootstrapServers")
  }

  def stop(): Future[Unit] = {
    serverBinding
      .flatMap(_.unbind())
      .flatMap { _ =>
        logger.info("TCP server stopped, stopping Kafka producer...")
        producer.close()
      }
      .flatMap(_ => system.terminate())
      .map(_ => ())
  }

  def server(address: String, port: Int): Future[Tcp.ServerBinding] = {
    val deciderFlow: Supervision.Decider = {
      case NonFatal(e) =>
        logger.info(s"Stream failed with: ${e.getMessage}, going to restart")
        Supervision.Restart
      case _ => Supervision.Stop
    }

    val frameUpToMllpTerminator = Framing.delimiter(
      ByteString(END_OF_BLOCK + CARRIAGE_RETURN),
      maximumFrameLength = 2048,
      allowTruncation = true)

    // Remove MLLP START_OF_BLOCK from this message,
    // because only END_OF_BLOCK + CARRIAGE_RETURN is removed by framing
    val scrubber = Flow[String]
      .map(StringUtils.stripStart(_, START_OF_BLOCK))

    val kafkaProducer: Flow[String, Either[Valid, Invalid], NotUsed] = Flow[String]
      .mapAsync(1) { each =>
        producer
          .send(new ProducerRecord(topic, partition0, null: String, each))
          .map[Either[Valid, Invalid]] { _ =>
            logger.info(s"Successfully sent to Kafka: ${printableShort(each)}.")
            Left(Valid(each))
          }
          .recover {
            case NonFatal(error) =>
              logger.error(s"Failed to send to Kafka. Sending NACK to client for element ${printableShort(each)}", error)
              Right(Invalid(each))
          }
      }

    // Parse the hairy HL7 message beast
    val hl7Parser = Flow[Either[Valid, Invalid]]
      .map {
        case Left(Valid(each)) =>
          try {
            logger.info("About to parse message: " + printableShort(each))
            val parser = getPipeParser(true)
            val message = parser.parse(each)
            logger.debug(s"Successfully parsed message in version: ${message.getVersion}")
            val ack = parser.encode(message.generateACK())
            encodeMllp(ack)
          } catch {
            case ex: HL7Exception =>
              val rootCause = ExceptionUtils.getRootCause(ex).getMessage
              logger.error(s"Error during parsing. Problem with message structure. Answer with NACK. Cause: $rootCause")
              encodeMllp(generateNACK(each))
            case ex: Throwable =>
              val rootCause = ExceptionUtils.getRootCause(ex).getMessage
              logger.error(s"Error during parsing. This should not happen. Answer with default NACK. Cause: $rootCause")
              encodeMllp(generateNACK(each))
          }
        case Right(Invalid(each)) =>
          encodeMllp(generateNACK(each))
      }

    val handler = Sink.foreach[Tcp.IncomingConnection] { connection =>
      val responseFlow = Flow[ByteString]
        .wireTap(each => logger.debug(s"Got message from client: ${connection.remoteAddress}. Size before framing: " + each.size))
        .via(frameUpToMllpTerminator)
        .wireTap(each => logger.debug("Size after framing: " + each.size))
        .map(_.utf8String)
        .via(scrubber)
        .via(kafkaProducer)
        .via(hl7Parser)
        .map(ByteString(_))
        .withAttributes(ActorAttributes.supervisionStrategy(deciderFlow))
        .watchTermination { (_, done) =>
          done.onComplete {
            case Failure(err) => logger.error(s"Server flow failed: $err")
            case _ => logger.debug(s"Server flow terminated for client: ${connection.remoteAddress}")
          }
        }
      connection.handleWith(responseFlow)
    }

    val binding = Tcp()
      .bind(interface = address, port = port)
      .watchTermination(Keep.left)
      .to(handler)
      .run()

    binding.onComplete {
      case Success(b) =>
        logger.info(s"Server started, listening on: ${b.localAddress}")
      case Failure(e) =>
        logger.info(s"Server could not bind to: $address:$port: ${e.getMessage}")
        system.terminate()
    }
    binding
  }

  private def generateNACK(each: String): String =
    getPipeParser().parse(each).generateACK(AcknowledgmentCode.AE, null).encode()

  private def getPipeParser(withValidation: Boolean = false) = {
    val context = new DefaultHapiContext

    // The ValidationContext is used during parsing as well as during
    // validation using {@link ca.uhn.hl7v2.validation.Validator} objects.

    // Set to false to do parsing without validation
    context.getParserConfiguration.setValidating(withValidation)
    // Set to false, because there is currently no separate validation step
    context.setValidationContext(ValidationContextFactory.noValidation)

    context.getPipeParser
  }

  def initializeTopic(topic: String): Future[RecordMetadata] =
    producer.send(new ProducerRecord(topic, partition0, null: String, InitialMsg))

  sys.ShutdownHookThread {
    logger.info("Got control-c cmd from shell or SIGTERM, about to shutdown...")
    stop()
  }
}

object Hl7Tcp2Kafka {
  lazy val server = new Hl7Tcp2Kafka()

  def main(args: Array[String]): Unit = {
    server.run()
  }

  def apply(mappedPort: Int): Hl7Tcp2Kafka = new Hl7Tcp2Kafka(mappedPort)

  def stop(): Future[Unit] = server.stop()
}

case class Valid(payload: String)

case class Invalid(payload: String)

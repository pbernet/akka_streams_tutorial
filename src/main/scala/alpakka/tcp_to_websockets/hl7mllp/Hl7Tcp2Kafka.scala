package alpakka.tcp_to_websockets.hl7mllp

import ca.uhn.hl7v2.validation.impl.ValidationContextFactory
import ca.uhn.hl7v2.{AcknowledgmentCode, DefaultHapiContext, HL7Exception}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.kafka.ProducerSettings
import org.apache.pekko.stream.scaladsl.{Flow, Framing, Keep, Sink, Tcp}
import org.apache.pekko.stream.{ActorAttributes, Supervision}
import org.apache.pekko.util.ByteString
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties
import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/**
  * PoC of an akka streams based HL7 MLLP listener with the same behaviour as [[Hl7MllpListener]]:
  *  - Receive HL7 messages over tcp
  *  - Frame according to MLLP protocol
  *  - Parse using HAPI parser (all validation switched off)
  *  - Send to Kafka
  *  - Reply to client with ACK or NACK (eg on connection problems)
  *
  * TODO Add HL7 over HTTP channel:
  * https://hapifhir.github.io/hapi-hl7v2/hapi-hl7overhttp/index.html
  *
  * TODO Avoid loosing messages in kafkaProducer (when Kafka is down) in a more graceful fashion
  * Switch to:
  * https://doc.akka.io/docs/alpakka-kafka/current/producer.html#producer-as-a-flow
  * when done:
  * https://github.com/akka/alpakka-kafka/issues/1101
  *
  */
class Hl7Tcp2Kafka(mappedPortKafka: Int = 9092) extends MllpProtocol {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val bootstrapServers = s"127.0.0.1:$mappedPortKafka"
  val topic = "hl7-input"
  //initial msg in topic, required to create the topic before any consumer subscribes to it
  val InitialMsg = "InitialMsg"
  val partition0 = 0
  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)
  val producer = initializeTopic(topic)
  val adminClient = initializeAdminClient(bootstrapServers)

  val (address, port) = ("127.0.0.1", 6160)
  var serverBinding: Future[Tcp.ServerBinding] = _

  def run() = {
    serverBinding = server(address, port)
    logger.info(s"Sending messages to Kafka on: $bootstrapServers")
  }

  def stop() = {
    serverBinding.map { b =>
      b.unbind().onComplete {
        _ =>
          logger.info("TCP server stopped, stopping producer flow...")
          system.terminate()
      }
    }
  }


  def server(address: String, port: Int): Future[Tcp.ServerBinding] = {

    val deciderFlow: Supervision.Decider = {
      case NonFatal(e) =>
        logger.info(s"Stream failed with: ${e.getMessage}, going to restart")
        Supervision.Restart
      case _ => Supervision.Stop
    }

    val frameUpToMllpTerminator = {
      Framing.delimiter(
        ByteString(END_OF_BLOCK + CARRIAGE_RETURN),
        maximumFrameLength = 2048,
        allowTruncation = true)
    }

    // Remove MLLP START_OF_BLOCK from this message,
    // because only END_OF_BLOCK + CARRIAGE_RETURN is removed by framing
    val scrubber = Flow[String]
      .map(each => StringUtils.stripStart(each, START_OF_BLOCK))

    val kafkaProducer: Flow[String, Either[Valid[String], Invalid[String]], NotUsed] = Flow[String]
      .map(each => {
        //Retry handling for Kafka producers is now built-in, see:
        //https://doc.akka.io/docs/alpakka-kafka/current/errorhandling.html#failing-producer
        //When using the built-in strategy, the buffers are filled when Kafka goes down and then messages are sent once Kafka recovers.
        //However, for this HL7 use case handling errors on the MLLP protocol level (by replying with a NACK) might be
        //a better strategy to give the HL7 sender a chance to re-send in-flight messages in case THIS client goes down as well.
        val topicExists = adminClient.listTopics.names.get.contains(topic)
        if (topicExists) {
          producer.send(new ProducerRecord(topic, partition0, null: String, each))
          logger.info(s"Successfully sent to Kafka: ${printableShort(each)}.")
          Left(Valid(each))
        } else {
          logger.error(s"Topic $topic does not exist or there is no connection to Kafka broker. Sending NACK to client for element ${printableShort(each)}")
          Right(Invalid(each, Some(new RuntimeException("Kafka connection problem. Sending NACK to client"))))
        }
      })

    // Parse the hairy HL7 message beast
    val hl7Parser = Flow[Either[Valid[String], Invalid[String]]]
      .map {
        case left@Left(_) =>
          val each = left.value.payload

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
        case _@Right(Invalid(each, _)) =>
          encodeMllp(generateNACK(each))
      }


    val handler = Sink.foreach[Tcp.IncomingConnection] { connection =>
      val serverEchoFlow = Flow[ByteString]
        .wireTap(each => logger.debug(s"Got message from client: ${connection.remoteAddress}. Size before framing: " + each.size))
        .via(frameUpToMllpTerminator)
        .wireTap(each => logger.debug("Size after framing: " + each.size))
        .map(_.utf8String)
        .via(scrubber)
        .via(kafkaProducer)
        .via(hl7Parser)
        .map(ByteString(_))
        .withAttributes(ActorAttributes.supervisionStrategy(deciderFlow))
        .watchTermination()((_, done) => done.onComplete {
          case Failure(err) => logger.error(s"Server flow failed: $err")
          case _ => logger.debug(s"Server flow terminated for client: ${connection.remoteAddress}")
        })
      connection.handleWith(serverEchoFlow)
    }

    val connections = Tcp().bind(interface = address, port = port)
    val binding = connections.watchTermination()(Keep.left).to(handler).run()

    binding.onComplete {
      case Success(b) =>
        logger.info(s"Server started, listening on: ${b.localAddress}")
      case Failure(e) =>
        logger.info(s"Server could not bind to: $address:$port: ${e.getMessage}")
        system.terminate()
    }
    binding
  }


  private def generateNACK(each: String) = {
    val nackString = getPipeParser().parse(each).generateACK(AcknowledgmentCode.AE, null).encode()
    nackString
  }

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

  def initializeTopic(topic: String): Producer[String, String] = {
    val producer = producerSettings.createKafkaProducer()
    producer.send(new ProducerRecord(topic, partition0, null: String, InitialMsg))
    producer
  }

  def initializeAdminClient(bootstrapServers: String) = {
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", bootstrapServers)
    AdminClient.create(prop)
  }

  sys.ShutdownHookThread {
    logger.info("Got control-c cmd from shell or SIGTERM, about to shutdown...")
    stop()
  }
}

object Hl7Tcp2Kafka extends App {
  val server = new Hl7Tcp2Kafka()
  server.run()

  def apply(mappedPort: Int) = new Hl7Tcp2Kafka(mappedPort)

  def stop() = server.stop()
}

case class Valid[T](payload: T)

case class Invalid[T](payload: T, cause: Option[Throwable])
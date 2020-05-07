package alpakka.tcp_to_websockets.hl7mllp

import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.stream.scaladsl.{Flow, Framing, Keep, Sink, Source, Tcp}
import akka.stream.{ActorAttributes, Supervision}
import akka.util.ByteString
import ca.uhn.hl7v2.validation.impl.ValidationContextFactory
import ca.uhn.hl7v2.{DefaultHapiContext, HL7Exception}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/**
  * PoC of an akka streams based HL7 MLLP listener with the same behaviour as [[Hl7MllpListener]]:
  *  - Receive HL7 messages over tcp
  *  - Frame according to MLLP
  *  - Parse using HAPI parser (all validation switched off)
  *  - Reply with ACK or NACK (if everything fails)
  *  - Send to Kafka
  *
  * Works with:
  *  - built in localClient
  *  - external client: [[Hl7MllpSender]]
  *
  * Doc:
  * http://hl7.ihelse.net/hl7v3/infrastructure/transport/transport_mllp.html
  * and [[ca.uhn.hl7v2.llp.MllpConstants]]
  *
  * TODO Add HL7 over HTTP:
  * https://hapifhir.github.io/hapi-hl7v2/hapi-hl7overhttp/index.html
  *
  * TODO Avoid loosing messages when Kafka is down
  * Switch to:
  * https://doc.akka.io/docs/alpakka-kafka/current/producer.html#producer-as-a-flow
  * even better
  * https://github.com/akka/alpakka-kafka/issues/1101
  *
  */
object Hl7MllpListenerAkkaStreams extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val system = ActorSystem("Hl7MllpListenerAkkaStreams")

  // Bootstrap Kafka client

  val bootstrapServers = "localhost:9092"
  val topic = "hl7-input"
  //initial msg in topic, required to create the topic before any consumer subscribes to it
  val InitialMsg = "InitialMsg"
  val partition0 = 0
  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)
  val producer = initializeTopic(topic)


  //MLLP: messages begin after hex "0x0B" and continue until "0x1C|0x0D"
  val START_OF_BLOCK = "\u000b" //0x0B
  val END_OF_BLOCK = "\u001c" //0x1C
  val CARRIAGE_RETURN = "\r" //0x0D

  val (address, port) = ("127.0.0.1", 6160)
  val serverBinding = server(system, address, port)
  (1 to 1).par.foreach(each => localClient(each, 1000, system, address, port))


  def server(system: ActorSystem, address: String, port: Int): Future[Tcp.ServerBinding] = {
    implicit val sys = system
    implicit val ec = system.dispatcher

    val deciderFlow: Supervision.Decider = {
      case NonFatal(e) =>
        logger.info(s"Stream failed with: ${e.getMessage}, going to restart")
        Supervision.Restart
      case _ => Supervision.Stop
    }

    // Remove MLLP START_OF_BLOCK from this message,
    // because only END_OF_BLOCK + CARRIAGE_RETURN is removed by framing
    val scrubber = Flow[String]
      .map(each => StringUtils.stripStart(each, START_OF_BLOCK))

    val kafkaProducer = Flow[String]
      .map(each => {
        producer.send(new ProducerRecord(topic, partition0, null: String, each))
        each
      })

    // Parse the hairy HL7 message beast
    val hl7Parser = Flow[String]
      .map(each => {
        val context = new DefaultHapiContext

        // The ValidationContext is used during parsing as well as during
        // validation using {@link ca.uhn.hl7v2.validation.Validator} objects.

        // Set to false to do parsing without validation
        context.getParserConfiguration.setValidating(false)
        // Set to false, because there is currently no separate validation step
        context.setValidationContext(ValidationContextFactory.noValidation)

        val parser = context.getPipeParser

        try {
          logger.info("About to parse message: " + printableShort(each))
          val message = parser.parse(each)
          logger.info("Successfully parsed")

          val ack = parser.encode(message.generateACK())
          encodeMllp(ack)
        } catch {
          case ex: HL7Exception =>
            val rootCause = printable(ExceptionUtils.getRootCause(ex).getMessage)
            logger.error(s"Error during parsing. Problem with message structure. Answer with NACK. Cause: $rootCause")
            //TODO Build NACK from message. Note that there is a piggyback ex.getResponseMessage which could be used
            val nack = "NACK"
            encodeMllp(nack)
          case ex: Throwable =>
            val rootCause = ExceptionUtils.getRootCause(ex).getMessage
            logger.error(s"Error during parsing. This should not happen. Answer with default NACK. Cause: $rootCause")
            //TODO Find a sensible default format
            val nack = "NACK"
            encodeMllp(nack)
        }
      })


    val handler = Sink.foreach[Tcp.IncomingConnection] { connection =>
      val serverEchoFlow = Flow[ByteString]
        .wireTap(_ => logger.debug(s"Got message from client: ${connection.remoteAddress}"))
        .wireTap(each => logger.debug("Size before framing: " + each.size))
        // frame input stream up to MLLP terminator: "END_OF_BLOCK + CARRIAGE_RETURN"
        .via(Framing.delimiter(
          ByteString(END_OF_BLOCK + CARRIAGE_RETURN),
          maximumFrameLength = 2048,
          allowTruncation = true))
        .wireTap(each => logger.debug("Size after framing: " + each.size))
        .map(_.utf8String)
        .via(scrubber)
        .via(kafkaProducer)
        .via(hl7Parser)
        .map(ByteString(_))
        .withAttributes(ActorAttributes.supervisionStrategy(deciderFlow))
        .watchTermination()((_, done) => done.onComplete {
          case Failure(err) => logger.info(s"Server flow failed: $err")
          case _ => logger.info(s"Server flow terminated for client: ${connection.remoteAddress}")
        })
      connection.handleWith(serverEchoFlow)
    }

    val connections = Tcp().bind(interface = address, port = port)
    val binding = connections.watchTermination()(Keep.left).to(handler).run()

    binding.onComplete {
      case Success(b) =>
        logger.info("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        logger.info(s"Server could not bind to: $address:$port: ${e.getMessage}")
        system.terminate()
    }
    binding
  }


  def localClient(id: Int, numberOfMesssages: Int, system: ActorSystem, address: String, port: Int): Unit = {
    implicit val sys = system
    implicit val ec = system.dispatcher

    val connection = Tcp().outgoingConnection(address, port)

    val hl7MllpMessages=  (1 to numberOfMesssages).map(each => ByteString(encodeMllp(generateTestMessage(each.toString)) ))
    val source = Source(hl7MllpMessages).throttle(10, 1.second).via(connection)
    val closed = source.runForeach(each => logger.info(s"Client: $id received echo: ${printable(each.utf8String)}"))
    closed.onComplete(each => logger.info(s"Client: $id closed: $each"))
  }

  private def generateTestMessage(senderTraceID: String) = {
    //For now put the senderTraceID into the "sender lab" field to follow the messages
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

  private def encodeMllp(message: String) = {
    START_OF_BLOCK + message + END_OF_BLOCK + CARRIAGE_RETURN
  }

  // The HAPI parser needs /r as segment terminator, but this is not printable
  private def printable(message: String): String = {
    message.replace("\r", "\n")
  }

  private def printableShort(message: String): String = {
    printable(message).take(20).concat("...")
  }

  def initializeTopic(topic: String): Producer[String, String] = {
   val producer = producerSettings.createKafkaProducer()
    producer.send(new ProducerRecord(topic, partition0, null: String, InitialMsg))
    producer
  }
}
package alpakka.tcp_to_websockets

import alpakka.env.{KafkaServerTestcontainers, WebsocketServer}
import alpakka.tcp_to_websockets.hl7mllp.{Hl7Tcp2Kafka, Hl7TcpClient}
import alpakka.tcp_to_websockets.websockets.{Kafka2SSE, Kafka2Websocket}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{BeforeAndAfterEachTestData, TestData}
import org.slf4j.{Logger, LoggerFactory}
import util.LogFileScanner

/**
  * Integration-Test class for example "HL7 V2 over TCP via Kafka to Websockets"
  *
  * Doc:
  * https://github.com/pbernet/akka_streams_tutorial#hl7-v2-over-tcp-via-kafka-to-websockets
  *
  * Remarks:
  *  - The test focus is on log file scanning to check for processed messages and ERRORs
  *  - This setup restarts Kafka for each test, so they can run independently. The downside
  *    of this is that we have to deal with a new mapped port on each restart.
  *    A setup with one Kafka start for all tests is here:
  *    https://doc.akka.io/docs/alpakka-kafka/current/testing-testcontainers.html
  *  - Since the shutdown of producers/consumers takes a long time, there are WARN msgs in the log
  */
final class AlpakkaTrophySpec extends AsyncWordSpec with Matchers with BeforeAndAfterEachTestData {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val kafkaContainer: KafkaServerTestcontainers = KafkaServerTestcontainers()
  var mappedPortKafka: Int = _

  var websocketServer: WebsocketServer = _
  var hl7Tcp2Kafka: Hl7Tcp2Kafka = _
  var kafka2Websocket: Kafka2Websocket = _
  var kafka2SSE: Kafka2SSE = _

  "Happy path" should {
    "find all processed messages in WebsocketServer log" in {
      val numberOfMessages = 10
      Hl7TcpClient(numberOfMessages)

      new LogFileScanner().run(10, 10, "Starting test: Happy path should find all processed messages in WebsocketServer log", "ERROR").length should equal(0)
      // 10 + 1 Initial message
      new LogFileScanner().run(10, 10, "Starting test: Happy path should find all processed messages in WebsocketServer log", "WebsocketServer received:").length should equal(numberOfMessages + 1)
    }
  }
  "NOT Happy path" should {
    "recover after Hl7Tcp2Kafka restart" in {
      val numberOfMessages = 10
      Hl7TcpClient(numberOfMessages)

      // Stopping after half of the msg are processed
      Thread.sleep(5000)

      logger.info("Re-starting Hl7Tcp2Kafka...")
      hl7Tcp2Kafka.stop()
      hl7Tcp2Kafka = Hl7Tcp2Kafka(mappedPortKafka)
      hl7Tcp2Kafka.run()

      // 10 + 1 Initial message
      new LogFileScanner().run(10, 10, "Starting test: NOT Happy path should recover after Hl7Tcp2Kafka restart", "WebsocketServer received:").length should be >= (numberOfMessages + 1)
    }

    "recover after Kafka2Websocket restart" in {
      val numberOfMessages = 10
      Hl7TcpClient(numberOfMessages)

      // Stopping after half of the msg are processed
      Thread.sleep(5000)

      logger.info("Re-starting Kafka2Websocket...")
      kafka2Websocket.stop()
      kafka2Websocket = Kafka2Websocket(mappedPortKafka)
      kafka2Websocket.run()

      // 10 + 1 Initial message
      new LogFileScanner().run(10, 10, "Starting test: NOT Happy path should recover after Kafka2Websocket restart", "WebsocketServer received:").length should be >= (numberOfMessages + 1)
    }
// OK, when started on its own. NOK when run in suite
//    "recover after WebsocketServer restart" in {
//      val numberOfMessages = 10
//      Hl7TcpClient(numberOfMessages)
//
//      // Stopping after half of the msg are processed
//      Thread.sleep(5000)
//
//      logger.info("Re-starting WebsocketServer...")
//      websocketServer.stop()
//      websocketServer = WebsocketServer()
//      websocketServer.run()
//
//      // The restart of the Kafka consumer and the recovery of the ws connection needs a long time...
//      // Unfortunately, even with the pessimistic connection check approach in Kafka2Websocket>>safeSendToWebsocket,
//      // due to the async sending via SourceQueue, we loose an in-flight message sometimes :-(
//      new LogFileScanner().run(20, 10, "Starting test: NOT Happy path should recover after WebsocketServer restart", "WebsocketServer received:").length should be >= (numberOfMessages + 1)
//    }

    "recover after Kafka restart" in {
      val numberOfMessages = 10
      Hl7TcpClient(numberOfMessages)

      // Stopping after half of the msg are processed
      Thread.sleep(5000)
      logger.info("Re-starting Kafka container...")
      kafkaContainer.stop()
      kafkaContainer.run()
      val newMappedPortKafka = kafkaContainer.mappedPort
      logger.info(s"Re-started Kafka on new mapped port: $newMappedPortKafka")

      // Now we need to restart the components sending/receiving to/from Kafka as well,
      // to connect to the new mapped port
      hl7Tcp2Kafka.stop()
      hl7Tcp2Kafka = Hl7Tcp2Kafka(newMappedPortKafka)
      hl7Tcp2Kafka.run()

      kafka2Websocket.stop()
      kafka2Websocket = Kafka2Websocket(newMappedPortKafka)
      kafka2Websocket.run()

      kafka2SSE.stop()
      kafka2SSE = Kafka2SSE(newMappedPortKafka)
      kafka2SSE.run()

      // 10 + 1 Initial message
      new LogFileScanner().run(30, 10, "Starting test: NOT Happy path should recover after Kafka restart", "WebsocketServer received:").length should be >= (numberOfMessages + 1)
    }
  }

  override protected def beforeEach(testData: TestData): Unit = {
    // Write start indicator for the LogFileScanner
    logger.info(s"Starting test: ${testData.name}")

    logger.info("Starting Kafka container...")
    kafkaContainer.run()
    mappedPortKafka = kafkaContainer.mappedPort
    logger.info(s"Running Kafka on mapped port: $mappedPortKafka")

    // Start other components
    websocketServer = WebsocketServer()
    websocketServer.run()

    hl7Tcp2Kafka = Hl7Tcp2Kafka(mappedPortKafka)
    hl7Tcp2Kafka.run()

    kafka2Websocket = Kafka2Websocket(mappedPortKafka)
    kafka2Websocket.run()

    kafka2SSE = Kafka2SSE(mappedPortKafka)
    kafka2SSE.run()
  }

  override protected def afterEach(testData: TestData): Unit = {
    logger.info("Stopping Kafka container...")
    kafkaContainer.stop()
    logger.info("Stopping other components...")
    websocketServer.stop()
    hl7Tcp2Kafka.stop()
    kafka2Websocket.stop()
    kafka2SSE.stop()
    // Grace time
    Thread.sleep(5000)
  }
}

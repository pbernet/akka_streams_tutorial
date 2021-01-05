import alpakka.env.{KafkaServerTestcontainers, WebsocketServer}
import alpakka.tcp_to_websockets.hl7mllp.{Hl7Tcp2Kafka, Hl7TcpClient}
import alpakka.tcp_to_websockets.websockets.Kafka2Websocket
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import util.LogFileScanner

/**
  * Integration-Test class for example "HL7 V2 over TCP via Kafka to Websockets"
  * Focus is on log file scanning to check for
  *
  * TODO Add not happy paths
  */
final class AlpakkaTrophySpec extends AsyncWordSpec with Matchers with BeforeAndAfterAll {
  val kafkaContainer = KafkaServerTestcontainers()

  "Happy path" should {
    "Produce HL7 messages and verify if they are received in WebsocketServer log" in {
      val numberOfMessages = 10
      Hl7TcpClient(numberOfMessages)
      new LogFileScanner().run(30, "WebsocketServer started, listening on", "WebsocketServer received:").length should equal(numberOfMessages + 1)
    }
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    println("Starting Kafka container...")
    kafkaContainer.run()
    val mappedPortKafka = kafkaContainer.mappedPort
    println(s"Running Kafka on mapped port: $mappedPortKafka")

    //Start other components
    WebsocketServer()
    Hl7Tcp2Kafka(mappedPortKafka)
    Kafka2Websocket(mappedPortKafka)
  }

  override protected def afterAll():Unit = {
    super.afterAll()
    println("Stopping Kafka container...")
    kafkaContainer.stop()
  }
}

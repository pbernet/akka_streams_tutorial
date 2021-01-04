import alpakka.env.KafkaServerTestcontainers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec



final class AlpakkaTrophySpec extends AsyncWordSpec with Matchers with BeforeAndAfterAll {
  val kafkaContainer = KafkaServerTestcontainers()


  "AlpakkaTrophySpec" should {
    "Start/Stop Kafka container" in {
      Thread.sleep(10000)
      assert(true)
    }
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    println("Starting Kafka container...")
    kafkaContainer.run()
    println(s"Running on mapped port: ${kafkaContainer.mappedPort}")

    //TODO Run other components
  }

  override protected def afterAll():Unit = {
    super.afterAll()
    println("Stopping Kafka container...")
    kafkaContainer.stop()
  }
}

package alpakka.env

import org.slf4j.{Logger, LoggerFactory}
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

/**
  * Uses testcontainers.org to run the
  * latest Kafka-Version from confluentinc
  *
  * Alternatives:
  *  - [[KafkaServer]]
  *  - https://doc.akka.io/docs/alpakka-kafka/current/testing-testcontainers.html
  *
  * Doc:
  * https://www.testcontainers.org/modules/kafka
  */
class KafkaServerTestcontainers {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  // Pin Kafka version for now, because 'latest' does not work on github actions anymore
  // https://hub.docker.com/r/confluentinc/cp-kafka
  // https://docs.confluent.io/platform/current/installation/versions-interoperability.html#cp-and-apache-kafka-compatibility
  val kafkaVersion = "6.2.1"
  val imageName = s"confluentinc/cp-kafka:$kafkaVersion"
  val originalPort = 9093
  var mappedPort = 1111
  val kafkaContainer = new KafkaContainer(DockerImageName.parse(imageName)).
    withExposedPorts(originalPort)

  def run() = {
    kafkaContainer.start()
    mappedPort = kafkaContainer.getMappedPort(originalPort)
    logger.info(s"Running Kafka: $imageName on mapped port: $mappedPort")
  }

  def stop() = {
    kafkaContainer.stop()
  }
}

object KafkaServerTestcontainers extends App {
  val server = new KafkaServerTestcontainers()
  server.run()

  sys.ShutdownHookThread{
    println("Got control-c cmd from shell or SIGTERM, about to shutdown...")
    server.stop()
  }

  Thread.currentThread.join()

  def apply(): KafkaServerTestcontainers = new KafkaServerTestcontainers()
  def mappedPort(): Int = server.mappedPort
}
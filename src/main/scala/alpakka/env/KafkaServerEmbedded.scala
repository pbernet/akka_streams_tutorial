package alpakka.env

import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}

/**
  * In-Memory Kafka broker (no persistence)
  *
  * Doc:
  * https://github.com/embeddedkafka/embedded-kafka
  *
  * Alternatives:
  *  - Run [[KafkaServerTestcontainers]] and adjust tmp port in producer/consumer classes
  *  - Setup Kafka server manually, see: https://kafka.apache.org/quickstart
  *  - Use Confluent Cloud, see: https://www.confluent.io/confluent-cloud/#view-pricing
  */
object KafkaServerEmbedded extends App {
  implicit val config = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)
  EmbeddedKafka.start()

  sys.addShutdownHook {
    println("Got control-c cmd from shell or SIGTERM, about to shutdown...")
    EmbeddedKafka.stop()
  }
}

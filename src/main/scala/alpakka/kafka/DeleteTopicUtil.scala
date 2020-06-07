package alpakka.kafka

import _root_.kafka.admin.TopicCommand

/**
  * Utility to reset the offset - only works if:
  * - No clients are connected to KafkaServer
  * - KafkaServer is running
  *
  * An alternative would probably be the reset tool:
  * https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
  */
object DeleteTopicUtil extends App {
  try {
    println("About to delete Topics...")
    val deleteargs1 = Array[String]("--zookeeper", "localhost:2181", "--delete", "--topic", "wordcount-input, wordcount-output")
    TopicCommand.main(deleteargs1)

    val deleteargs2 = Array[String]("--zookeeper", "localhost:2181", "--delete", "--topic", "messagecount-output")
    TopicCommand.main(deleteargs2)

  } catch {
    case e @ (_ : RuntimeException ) => println("Boom" + e)
  }
}

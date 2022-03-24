package alpakka.kafka

import _root_.kafka.admin.TopicCommand

/**
  * Utility to reset the offset - only works if:
  * - No clients are connected to KafkaServer
  * - KafkaServer is running and Zookeeper is reachable on port 2181
  *
  * An alternative would probably be the reset tool:
  * https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
  */
object DeleteTopicUtil extends App {
  try {
    println("About to delete topics...")
    val deleteargs1 = Array[String]("--bootstrap-server", "localhost:2181", "--delete", "--topic", "wordcount-input, wordcount-output")
    TopicCommand.main(deleteargs1)

    val deleteargs2 = Array[String]("--bootstrap-server", "localhost:2181", "--delete", "--topic", "messagecount-output")
    TopicCommand.main(deleteargs2)

  } catch {
    case e @ (_ : RuntimeException ) => println("Ex during topic delete: " + e)
  }
}

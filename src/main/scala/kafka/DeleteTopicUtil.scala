package kafka

import _root_.kafka.admin.TopicCommand

/**
  * Utility to reset the offset - only works if:
  * - No clients are connected to KafkaServer
  * - KafkaServer is running
  *
  * TODO Currently only works for one topic
  */
object DeleteTopicUtil extends App {
  try {
    println("About to delete...")
    val deleteargs1 = Array[String]("--zookeeper", "localhost:2181", "--delete", "--topic", "wordcount-input")
    val deleteargs2 = Array[String]("--zookeeper", "localhost:2181", "--delete", "--topic", "wordcount-output")

    TopicCommand.main(deleteargs1)
    TopicCommand.main(deleteargs2)
  } catch {
    case e @ (_ : RuntimeException ) => println("Boom" + e)
  }
}

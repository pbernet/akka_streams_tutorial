package kafka

import java.net.InetSocketAddress
import java.nio.file.Files
import java.util.Properties

import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.zookeeper.server.quorum.QuorumPeerConfig
import org.apache.zookeeper.server.{ServerConfig, ZooKeeperServerMain}

/**
  * Default KafkaServer which embedded Zookeeper, inspired by:
  * https://github.com/jamesward/koober/blob/master/kafka-server/src/main/scala/KafkaServer.scala
  *
  */
object KafkaServer extends App {

  val quorumConfiguration = new QuorumPeerConfig {
    override def getDataDir: String = Files.createTempDirectory("zookeeper").toString
    override def getDataLogDir: String = Files.createTempDirectory("zookeeper-logs").toString
    override def getClientPortAddress: InetSocketAddress = new InetSocketAddress(2181)
  }

  class StoppableZooKeeperServerMain extends ZooKeeperServerMain {
    def stop(): Unit = shutdown()
  }

  val zooKeeperServer = new StoppableZooKeeperServerMain()

  val zooKeeperConfig = new ServerConfig()
  zooKeeperConfig.readFrom(quorumConfiguration)

  val zooKeeperThread = new Thread {
    override def run(): Unit = zooKeeperServer.runFromConfig(zooKeeperConfig)
  }

  zooKeeperThread.start()

  //Maybe not all the properties below are needed, they were copied from a working standalone Kafka installation
  val kafkaProperties = new Properties()
  kafkaProperties.put("zookeeper.connect", "localhost:2181")
  kafkaProperties.put("broker.id", "0")
  kafkaProperties.put("offsets.topic.replication.factor", "1")
  kafkaProperties.put("log.dirs", Files.createTempDirectory("kafka-logs").toString)
  kafkaProperties.put("delete.topic.enable", "true")
  kafkaProperties.put("group.initial.rebalance.delay.ms", "0")
  kafkaProperties.put("group.initial.rebalance.delay.ms", "0")
  kafkaProperties.put("transaction.state.log.min.isr", "1")
  kafkaProperties.put("transaction.state.log.replication.factor", "1")
  kafkaProperties.put("zookeeper.connection.timeout.ms", "6000")

  val kafkaConfig = KafkaConfig.fromProps(kafkaProperties)

  val kafka = new KafkaServerStartable(kafkaConfig)

  println("About to start...")
  kafka.startup()

  sys.addShutdownHook{
    println("About to shutdown...")
    kafka.shutdown()
    kafka.awaitShutdown()
    zooKeeperServer.stop()
  }

  zooKeeperThread.join()
}

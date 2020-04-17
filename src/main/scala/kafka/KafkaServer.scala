package kafka

import java.io.File
import java.net.InetSocketAddress
import java.nio.file.{Files, Paths}
import java.util.Properties

import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.commons.io.FileUtils
import org.apache.zookeeper.server.quorum.QuorumPeerConfig
import org.apache.zookeeper.server.{ServerConfig, ZooKeeperServerMain}

/**
  * Kafka which embedded Zookeeper, inspired by:
  * https://github.com/jamesward/koober/blob/master/kafka-server/src/main/scala/KafkaServer.scala
  *
  * Remarks:
  *  - Shut down gracefully via exit (so that shutdown hook runs)
  *  - Zookeeper starts an admin server on http://localhost:8080/commands
  *
  * Alternatives:
  *  - Setup Kafka server manually, see: https://kafka.apache.org/quickstart
  *  - "Embedded Kafka", see: https://github.com/manub/scalatest-embedded-kafka
  *  - Run Kafka in docker
  *    see: https://github.com/wurstmeister/kafka-docker
  *    or together with kafdrop admin console: https://towardsdatascience.com/kafdrop-e869e5490d62
  *  - Use Confluent Cloud, see: https://www.confluent.io/confluent-cloud/#view-pricing
  */
object KafkaServer extends App {

  val zookeeperPort = 2181

  val zooKeeperRoot =  "/tmp/zookeeper"
  val zooKeeperRootPath =  Paths.get(zooKeeperRoot) //Resolves to "C:\tmp" on windows
  val kafkaRoot = "/tmp/kafka-logs"
  val kafkaRootPath = Paths.get(kafkaRoot)

  def removeDataDirs(): Unit = {
    if(zooKeeperRootPath.toFile.exists()) FileUtils.forceDelete(zooKeeperRootPath.toFile)
    if(kafkaRootPath.toFile.exists()) FileUtils.forceDelete(kafkaRootPath.toFile)
  }

  //removeDataDirs()

  val quorumConfiguration = new QuorumPeerConfig {
    override def getDataDir: File = Files.createDirectories(zooKeeperRootPath.resolve("zookeeper")).toFile
    override def getDataLogDir: File = Files.createDirectories(zooKeeperRootPath.resolve("zookeeper-logs")).toFile
    override def getClientPortAddress: InetSocketAddress = new InetSocketAddress(zookeeperPort)
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

  // Maybe not all the properties below are needed, they were copied from a working standalone Kafka installation
  val kafkaProperties = new Properties()
  kafkaProperties.put("zookeeper.connect", s"localhost:$zookeeperPort")
  kafkaProperties.put("broker.id", "0")
  kafkaProperties.put("offsets.topic.replication.factor", "1")
  kafkaProperties.put("log.dirs", kafkaRoot)
  kafkaProperties.put("delete.topic.enable", "true")
  kafkaProperties.put("group.initial.rebalance.delay.ms", "0")
  kafkaProperties.put("transaction.state.log.min.isr", "1")
  kafkaProperties.put("transaction.state.log.replication.factor", "1")
  kafkaProperties.put("zookeeper.connection.timeout.ms", "6000")
  kafkaProperties.put("num.partitions", "10")

  val kafkaConfig = KafkaConfig.fromProps(kafkaProperties)

  val kafka = new KafkaServerStartable(kafkaConfig)

  println("About to start...")
  kafka.startup()

  scala.sys.addShutdownHook{
    println("About to shutdown...")
    kafka.shutdown()
    kafka.awaitShutdown()
    zooKeeperServer.stop()
  }

  zooKeeperThread.join()
}

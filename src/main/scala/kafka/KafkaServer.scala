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
  * KafkaServer which embedded Zookeeper, inspired by:
  * https://github.com/jamesward/koober/blob/master/kafka-server/src/main/scala/KafkaServer.scala
  *
  * Remarks:
  *  - Zookeeper starts an admin server on http://localhost:8080/commands
  *  - Shut down gracefully via exit (so that shutdown hook runs)
  *
  * Alternatives:
  *  - Use "Embedded Kafka", see: https://github.com/manub/scalatest-embedded-kafka
  *  - Run Kafka in docker
  *    see: https://github.com/wurstmeister/kafka-docker
  *    or together with kafdrop admin console: https://towardsdatascience.com/kafdrop-e869e5490d62
  *  - Setup Kafka server manually, see: https://kafka.apache.org/quickstart
  *  - Use Confluent Cloud, see: https://www.confluent.io/confluent-cloud/#view-pricing
  */
object KafkaServer extends App {

  val zookeeperPort = 2181

  val kafkaLogs = "/tmp/kafka-logs"
  val kafkaLogsPath = Paths.get(kafkaLogs)

  // See: https://stackoverflow.com/questions/59592518/kafka-broker-doesnt-find-cluster-id-and-creates-new-one-after-docker-restart/60864763#comment108382967_60864763
  def fix25Behaviour() = {
    val fileWithConflictingContent = kafkaLogsPath.resolve("meta.properties").toFile
    if (fileWithConflictingContent.exists())  FileUtils.forceDelete(fileWithConflictingContent)
  }

  def removeKafkaLogs(): Unit = {
    if (kafkaLogsPath.toFile.exists()) FileUtils.forceDelete(kafkaLogsPath.toFile)
  }

  // Keeps the persistent data
  fix25Behaviour()
  // If everything fails
  //removeKafkaLogs()

  val quorumConfiguration = new QuorumPeerConfig {
    // Since we do not run a cluster, we are not interested in zookeeper data
    override def getDataDir: File = Files.createTempDirectory("zookeeper").toFile
    override def getDataLogDir: File = Files.createTempDirectory("zookeeper-logs").toFile
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

  val kafkaProperties = new Properties()
  kafkaProperties.put("zookeeper.connect", s"localhost:$zookeeperPort")
  kafkaProperties.put("broker.id", "0")
  kafkaProperties.put("offsets.topic.replication.factor", "1")
  kafkaProperties.put("log.dirs", kafkaLogs)
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

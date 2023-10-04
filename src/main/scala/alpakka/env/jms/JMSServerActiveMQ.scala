package alpakka.env.jms

import org.apache.activemq.broker.{BrokerPlugin, BrokerService}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Embedded old school ActiveMQ JMSServer to experiment with:
  *  - KahaDB persistence (in java.io.tmpdir)
  *  - AES encryption for the payload via AESBrokerPlugin
  *
  * Alternative: Embedded Artemis JMSServer [[JMSServerArtemis]]
  * https://activemq.apache.org/components/artemis/documentation
  * Search for: Embedding Apache ActiveMQ Artemis
  *
  * Issues:
  * Messages are processed but produces random NPE while decrypting messages in AESBroker
  *
  */
object JMSServerActiveMQ extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val broker = new BrokerService()
  val host: String = "localhost"
  val port = 21616
  val serverUrl = s"tcp://$host:$port"

  broker.addConnector(serverUrl)
  broker.setBrokerName(host)

  broker.setPersistent(true)
  broker.setDataDirectory(System.getProperty("java.io.tmpdir"))

  val aesPlugin = new AESBrokerPlugin()
  broker.setPlugins(Array[BrokerPlugin](aesPlugin))
  // For now the secret is passed via JVM system property
  System.setProperty("activemq.aeskey", "1234567890123456")

  broker.setAdvisorySupport(false)
  broker.setUseJmx(true)
  broker.setSchedulerSupport(false)

  broker.setUseShutdownHook(true)
  broker.start()

  if (broker.isStarted) {
    logger.info(s"JMSServer is started with available processors: ${Runtime.getRuntime.availableProcessors()}")
  }

  Thread.currentThread.join()
}

package alpakka.env.jms

import org.apache.activemq.broker.{BrokerPlugin, BrokerService}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Embedded ActiveMQ JMSServer for local testing with:
  *  - KahaDB persistence
  *  - AES encryption
  *
  * Alternative: Embedded Artemis JMSServer
  * https://activemq.apache.org/components/artemis/documentation/1.0.0/embedding-activemq.html
  *
  */
object JMSServer extends App {
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

package alpakka.env

import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager
import org.apache.activemq.artemis.spi.core.security.jaas.InVMLoginModule
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalDateTime
import java.util.Properties
import javax.jms.{ConnectionFactory, Queue, Session, TextMessage}
import javax.naming.{Context, InitialContext}

/**
  * Embedded Artemis JMSServer for local testing
  *
  * Config: resources/broker.xml
  *
  * Doc:
  * https://activemq.apache.org/components/artemis/documentation/1.1.0/embedding-activemq.html
  * https://github.com/apache/activemq-artemis/blob/main/examples/features/standard/embedded-simple/src/main/java/org/apache/activemq/artemis/jms/example/EmbeddedExample.java
  * https://github.com/apache/activemq-artemis/tree/master/examples/features/standard/embedded-simple
  *
  */
object JMSServerArtemis extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val host: String = "127.0.0.1"
  val port = 21616
  val serverUrl = s"tcp://$host:$port"

  val securityConfig = new SecurityConfiguration()
  securityConfig.addUser("artemis", "simetraehcapa")
  securityConfig.addRole("artemis", "guest")
  // Needed when run with broker_docker.xml
  securityConfig.addRole("artemis", "amq")
  securityConfig.setDefaultUser("artemis")
  val securityManager = new ActiveMQJAASSecurityManager(classOf[InVMLoginModule].getName, securityConfig)

  val broker = new EmbeddedActiveMQ()
  broker.setConfigResourcePath("broker.xml")
  broker.setSecurityManager(securityManager)
  broker.start()

  runTestClient()
  runTestClientJNDI()

  private def runTestClient(): Unit = {
    val cf = new ActiveMQConnectionFactory(serverUrl)
    val connection = cf.createConnection()
    connection.start()

    try {
      val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
      val bootstrapTestQueue = session.createQueue("jms.queue.bootstrapTestQueue")

      val producer = session.createProducer(bootstrapTestQueue)
      val message = session.createTextMessage("Test msg sent at: " + LocalDateTime.now())
      logger.info("About to send: " + message.getText)
      producer.send(message)

      val messageConsumer = session.createConsumer(bootstrapTestQueue)
      val messageReceived = messageConsumer.receive(10000).asInstanceOf[TextMessage]
      logger.info("Received message: " + messageReceived.getText)
    } finally {
      connection.close()
    }
  }

  private def runTestClientJNDI(): Unit = {
    // Picks up jndi.properties from resources
    val props = new Properties()
    props.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory")
    props.setProperty(Context.PROVIDER_URL, serverUrl)
    val initialContext = new InitialContext(props)

    // Use queue configured jndi.properties
    val queue = initialContext.lookup("queue/bootstrapTestQueueJNDI").asInstanceOf[Queue]
    val cf = initialContext.lookup("ConnectionFactory").asInstanceOf[ConnectionFactory]
    val connection = cf.createConnection()
    connection.start()

    try {
      val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
      val producer = session.createProducer(queue)
      val message = session.createTextMessage("Test msg JNDI sent at:" + LocalDateTime.now())
      logger.info("About to send: " + message.getText)
      producer.send(message)
      val messageConsumer = session.createConsumer(queue)

      val messageReceived = messageConsumer.receive(1000).asInstanceOf[TextMessage]
      logger.info("Received message JNDI:" + messageReceived.getText)
    } finally {
      connection.close()
    }
  }

  System.in.read
  broker.stop()
}
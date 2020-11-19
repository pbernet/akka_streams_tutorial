package alpakka.env

import org.apache.activemq.broker.BrokerService

/**
  * Embedded ActiveMQ JMSServer for local testing
  *
  * Alternative: Embedded Artemis JMSServer
  * https://activemq.apache.org/components/artemis/documentation/1.0.0/embedding-activemq.html
  *
  */
object JMSServer extends App {
  val broker = new BrokerService()
  val host: String = "localhost"
  val port = 21616
  val serverUrl = s"tcp://$host:$port"
  broker.addConnector(serverUrl)

  broker.setSchedulerSupport(false)
  broker.setPersistent(true)
  broker.setDataDirectory("/tmp")
  broker.setBrokerName(host)
  broker.setAdvisorySupport(false)
  broker.setUseJmx(true)
  broker.start()

  if (broker.isStarted) {
    println(s"JMSServer is started: ${broker.isStarted}")
  }

  System.in.read
  broker.stop()
  broker.waitUntilStopped()
}
package alpakka.env

import org.apache.activemq.broker.BrokerService

/**
  * Dummy JMSServer for local testing
  */
object JMSServer extends App {
  val broker = new BrokerService()
  val host: String = "localhost"
  val port = 21616
  val serverUrl = s"tcp://$host:$port"
  broker.addConnector(serverUrl)

  broker.setSchedulerSupport(false)
  broker.setPersistent(false)
  broker.setBrokerName(host)
  broker.setAdvisorySupport(false)
  broker.start()

  if (broker.isStarted) {
    println(s"JMSServer is started: ${broker.isStarted}")
  }

  System.in.read
  broker.stop()
  broker.waitUntilStopped()
}
package alpakka.jms

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.apache.activemq.broker.BrokerService

/**
  * Dummy JMSServer for local testing
  */
object JMSServer extends App {
  implicit val system = ActorSystem("JMSServer")
  implicit val executionContext = system.dispatcher
  implicit val materializerServer = ActorMaterializer()

  val broker = new BrokerService()
  val host: String = "localhost"
  val port = 8888
  val serverUrl = s"tcp://$host:$port"
  broker.addConnector(serverUrl)

  broker.setPersistent(false)
  broker.setBrokerName(host)
  broker.setUseJmx(false)
  broker.start()

  if (broker.isStarted) {
    println(s"JMSServer is started: ${broker.isStarted}")
  }
}
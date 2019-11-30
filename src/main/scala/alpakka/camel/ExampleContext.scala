package alpakka.camel

import org.apache.camel.impl.{DefaultCamelContext, SimpleRegistry}
import streamz.camel.StreamContext

trait ExampleContext {
  private val camelRegistry = new SimpleRegistry
  private val camelContext = new DefaultCamelContext

  camelContext.start()
  camelContext.setRegistry(camelRegistry)
  camelRegistry.put("exampleService", new ExampleService)

  implicit val context: StreamContext =
    StreamContext(camelContext)

  val tcpEndpointUri: String =
    "netty4:tcp://localhost:5150?sync=false&textline=true&encoding=utf-8"

  val fileEndpointUri: String =
    "file:input?charset=utf-8"

  val serviceEndpointUri: String =
    "bean:exampleService?method=linePrefix"

  val printerEndpointUri: String =
    "stream:out"
}

class ExampleService {
  def linePrefix(lineNumber: Int): String = s"[$lineNumber] "
}
package alpakka.camel

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import streamz.camel.StreamContext
import streamz.camel.akka.scaladsl._

import scala.collection.immutable.Seq

/**
  * PoC to verify apache camel CXF endpoint access from akka-streams via streamz lib
  * Requests are made against resources/accountservice.wsdl which is mocked in Castlemock docker image:
  * https://hub.docker.com/r/castlemock/castlemock
  *
  * Status:
  * Although the camel component/endpoint are created according to the log statements,
  * the processing hangs on the first request at the {scaladsl produce} method. Occurs also if WS endpoint is down
  *
  * Possible causes:
  * Incorrect "Camel type converter" for request/response?
  * Wrong endpoint URI - https://camel.apache.org/manual/latest/faq/how-do-i-configure-endpoints.html
  *
  * Doc Camel cxf endpoint:
  * https://camel.apache.org/components/latest/cxf-component.html
  *
  * Doc streamz DSL:
  * https://github.com/krasserm/streamz/blob/master/streamz-camel-akka/README.md
  *
  * Doc client test code examples:
  * https://github.com/krasserm/streamz/blob/0a01ff792951f224aba556dac21ac6311184642d/streamz-camel-akka/src/test/scala/streamz/camel/akka/scaladsl/ScalaDslSpec.scala
  *
  */
object Cxf extends App {
  implicit val system = ActorSystem("Cxf")

  //No need to create camel context etc...
  //private val camelContext = new DefaultCamelContext

  val cxfEndpointUri: String = "cxf://localhost:8080/castlemock/mock/soap/project/dyMt6D/AccountsPort?operationName=GetAccountDetails"
  //val cxfEndpointUri: String = "cxf://localhost:8080/castlemock/mock/soap/project/dyMt6D/AccountsPort"

  //Sth fails silently here - maybe not needed
  //val cxfEndpoint = new CxfEndpoint
  //camelContext.addEndpoint(cxfEndpointUri, cxfEndpoint)

  implicit val streamContext: StreamContext = StreamContext()


  // CXF service send
  val source = Source(Seq("1", "2", "3"))
  val printSink = Sink.foreach[String] { each: String => println(s"Finished processing: $each") }

  val flow = source
    .map{each => println("About to request: " + each); each}
    .sendRequest[String](cxfEndpointUri, parallelism = 1)

  flow.runWith(printSink)
}
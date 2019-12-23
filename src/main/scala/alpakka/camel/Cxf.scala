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
  * TODO example calling camel cxf endpoint example from camel repo
  * https://github.com/apache/camel/tree/master/examples/camel-example-cxf-proxy
  *
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
  implicit val streamContext: StreamContext = StreamContext()

  //TODO analyse URI param

  val cxfEndpointUri = "cxf://http://localhost:8080/castlemock/mock/soap/project/EG8XBV/AccountsPort?wsdlURL=src/main/resources/accountservice.wsdl&dataFormat=PAYLOAD&loggingFeatureEnabled=true&operationName=GetAccountDetails"

  val request =
    """
      |<x:Envelope xmlns:x="http://www.w3.org/2003/05/soap-envelope" xmlns:acc="http://com/blog/samples/webservices/accountservice">
      |    <x:Header/>
      |    <x:Body>
      |        <acc:AccountDetailsRequest>
      |            <acc:accountNumber>1</acc:accountNumber>
      |        </acc:AccountDetailsRequest>
      |    </x:Body>
      |</x:Envelope>
       """.stripMargin

  // CXF service send
  val source = Source(Seq(request, request, request))
  val printSink = Sink.foreach[String] { each: String => println(s"Finished processing: $each") }

  val flow = source
    .map{each => println("About to send request: " + each); each}
    //TODO It hangs here, even if WS is not up
    .sendRequest[String](cxfEndpointUri, parallelism = 1)

  flow.runWith(printSink)
}
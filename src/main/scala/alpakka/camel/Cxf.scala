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
  * we get an 200 response but with an unknown error
  *
  * Possible causes:
  * Wrong MESSAGE format
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
  implicit val streamContext: StreamContext = StreamContext()

  //TODO verifiy options for chosen dataFormat=MESSAGE
  val cxfEndpointUri = "cxf://http://localhost:8080/castlemock/mock/soap/project/EG8XBV/AccountsPort?wsdlURL=src/main/resources/accountservice.wsdl&dataFormat=MESSAGE&loggingFeatureEnabled=true&defaultOperationName=GetAccountDetails"

  val request =
    """<x:Envelope xmlns:x="http://www.w3.org/2003/05/soap-envelope" xmlns:acc="http://www.w3.org/2003/05/soap-envelope">
      |    <x:Body>
      |        <acc:AccountDetailsRequest>
      |            <acc:accountNumber>1</acc:accountNumber>
      |        </acc:AccountDetailsRequest>
      |    </x:Body>
      |</x:Envelope>""".stripMargin

  // CXF service send
  val source = Source(Seq(request, request, request))
  val printSink = Sink.foreach[String] { each: String => println(s"Finished processing: $each") }

  val flow = source
    .map{each => println("About to send request: " + each); each}
    //TODO We get an 200 response (with an error)
    //If WS is not up, camel does not complain
    .sendRequest[String](cxfEndpointUri, parallelism = 1)

  flow.runWith(printSink)
}
package alpakka.camel

import akka.Done
import akka.actor.ActorSystem
import org.slf4j.{Logger, LoggerFactory}
import streamz.camel.akka.scaladsl.{receiveRequest, receiveRequestBody, _}
import streamz.camel.{StreamContext, StreamMessage}

import scala.concurrent.Future

/**
  * Example echo implementations
  *
  * Doc: https://github.com/krasserm/streamz
  *
  */
object Greeter extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("Greeter")
  implicit val executionContext = system.dispatcher
  implicit val streamContext = StreamContext()

  // TCP greeter service. Use with:
  // telnet 0.0.0.0 5150
  receiveRequestBody[String, String]("netty4:tcp://0.0.0.0:5150?textline=true")
    .map(s => s"hello $s")
    .reply.run()

  // HTTP greeter service. Use with:
  // curl "http://0.0.0.0:8080/greeter?name=abc"
  receiveRequest[String, String]("jetty:http://0.0.0.0:8080/greeter")
    .map(msg => StreamMessage(s"Hello ${msg.headers.getOrElse("name", "unknown")}\n"))
    .reply.run()

  // MLLP HL7 async greeter. Use with Hl7TcpClient
  // https://camel.apache.org/components/latest/mllp-component.htm
  // When msgs are sent with Hl7TcpClient, we get an ACK from the MLLP component in Hl7TcpClient
  // TODO unable to hook into the flow here, not with Breakpoints nor Logging
  val done: Future[Done] = receiveBody[String]("mllp:0.0.0.0:6160")
    .wireTap(each => logger.info("Gotten: " + each))
    .run()

  done.onComplete {each => logger.info("Done")}

  //Alternative Implementation

  //  val tcpEndpointUri: String =
  //    "mllp:0.0.0.0:6160"
  //
  //  val printerEndpointUri: String =
  //    "stream:out"
  //  val tcpLineSource: Source[String, NotUsed] =
  //    receiveBody[String](tcpEndpointUri)
  //
  //  val stream: Source[String, NotUsed] =
  //    tcpLineSource
  //      .wireTap(each => logger.info("Gotten: " + each))
  //      .send(printerEndpointUri)
  //
  //  stream.runWith(Sink.ignore)


}

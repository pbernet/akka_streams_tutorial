package alpakka.camel

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import streamz.camel.akka.scaladsl.{receiveRequest, receiveRequestBody, _}
import streamz.camel.{StreamContext, StreamMessage}

/**
  * 1:1 example taken from streamz lib as a starting point
  *
  * Doc: https://github.com/krasserm/streamz
  *
  */
object Greeter extends App {
  implicit val system = ActorSystem("Greeter")
  implicit val streamContext = StreamContext()

  val printerEndpointUri: String =
    "stream:out"

  val tcpEndpointUri: String =
    "mllp:0.0.0.0:6160"

  val tcpLineSource: Source[String, NotUsed] =
    receiveBody[String](tcpEndpointUri)

  val stream: Source[String, NotUsed] =
    tcpLineSource
      .wireTap(each => println("Gotten: " + each))
      .send(printerEndpointUri)

  stream.runWith(Sink.ignore)



  // TCP greeter service. Use with e.g. telnet 0.0.0.0 5150
  receiveRequestBody[String, String]("netty4:tcp://0.0.0.0:5150?textline=true")
    .map(s => s"hello $s")
    .reply.run()

  // MLLP HL7 consumer. TODO Reply may not work because ACK is done under the hood?
  receiveRequestBody[String, String]("mllp:0.0.0.0:6160")
    .wireTap(each => println("Gotten: " + each))
    //Send HL7 Ack? or is it autoAck?
    .map(s => s"Ack $s")
    .reply.run()

  // HTTP greeter service. Use with e.g. curl "http://0.0.0.0:8080/greeter?name=abc"
  receiveRequest[String, String]("jetty:http://0.0.0.0:8080/greeter")
    .map(msg => StreamMessage(s"Hello ${msg.headers.getOrElse("name", "unknown")}\n"))
    .reply.run()
}

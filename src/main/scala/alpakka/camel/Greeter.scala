package alpakka.camel

import akka.actor.ActorSystem
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

  // TCP greeter service. Use with e.g. telnet 0.0.0.0 5150
  receiveRequestBody[String, String]("netty4:tcp://0.0.0.0:5150?textline=true")
    .map(s => s"hello $s")
    .reply.run()

  // HTTP greeter service. Use with e.g. curl "http://0.0.0.0:8080/greeter?name=abc"
  receiveRequest[String, String]("jetty:http://0.0.0.0:8080/greeter")
    .map(msg => StreamMessage(s"Hello ${msg.headers.getOrElse("name", "unknown")}\n"))
    .reply.run()
}

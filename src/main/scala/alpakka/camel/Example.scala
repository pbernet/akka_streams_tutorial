package alpakka.camel

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import streamz.camel.akka.scaladsl.{receiveBody, _}

import scala.collection.compat._
import scala.collection.immutable.Iterable


/**
  * 1:1 example taken from streamz lib as a starting point
  *
  * Doc:
  * https://github.com/krasserm/streamz/tree/master/streamz-examples
  *
  */
object Example extends ExampleContext with App {
  implicit val system = ActorSystem("Example")

  val tcpLineSource: Source[String, NotUsed] =
    receiveBody[String](tcpEndpointUri)

  val fileLineSource: Source[String, NotUsed] =
    receiveBody[String](fileEndpointUri).mapConcat(_.linesIterator.to(Iterable))

  val linePrefixSource: Source[String, NotUsed] =
    Source.fromIterator(() => Iterator.from(1)).sendRequest[String](serviceEndpointUri)

  val stream: Source[String, NotUsed] =
    tcpLineSource
      .merge(fileLineSource)
      .zipWith(linePrefixSource)((l, n) => n concat l)
      .send(printerEndpointUri)

  stream.runWith(Sink.ignore)
}

package sample.stream

import java.time.{Instant, ZoneId, ZonedDateTime}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.{DelayOverflowStrategy, ThrottleMode}

import scala.concurrent.duration._
import scala.util.Failure

case class SourceEvent(id: Integer)
case class DomainEvent(id: Integer, timeDate: ZonedDateTime)


/**
  * Inspired by:
  * https://doc.akka.io/docs/akka/2.5/stream/stream-cookbook.html#dropping-elements
  * https://github.com/DimaD/akka-streams-slow-consumer/blob/master/src/main/scala/Example.scala
  *
  */
object SlowConsumerDropsElementsOnFastProducer {
  implicit val system = ActorSystem("SlowConsumerDropsElementsOnFastProducer")
  implicit val ec = system.dispatcher

  def main(args: Array[String]): Unit = {
    fastSource
      .via(droppyStream)
      .via(enrichWithTimestamp)
      .watchTermination(){(_, done) => done.onComplete {
        case Failure(err) => println(s"Flow failed: $err")
        case _ => system.terminate(); println(s"Flow terminated")
      }}
      .runWith(slowSink)
  }

  val slowSink: Sink[DomainEvent, NotUsed] =
    Flow[DomainEvent]
      //.buffer(100, OverflowStrategy.backpressure)
      .delay(10.seconds, DelayOverflowStrategy.backpressure)
      .to(Sink.foreach(e => println(s"Reached Sink: $e")))

  val fastSource: Source[SourceEvent, NotUsed] =
    Source(1 to 500)
      .throttle(10, 1.second, 1, ThrottleMode.shaping)
      .map { i =>
        println(s"Producing event: $i")
        SourceEvent(i)
      }

  val enrichWithTimestamp: Flow[SourceEvent, DomainEvent, NotUsed] =
    Flow[SourceEvent]
      .map { e =>
        val instant = Instant.ofEpochMilli(System.currentTimeMillis())
        val zonedDateTimeUTC: ZonedDateTime = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"))
        DomainEvent(e.id, zonedDateTimeUTC)
      }

  val droppyStream: Flow[SourceEvent, SourceEvent, NotUsed] =
    //Conflate is "rate aware", it combines elements from upstream while downstream backpressures
    //The reducer function takes the freshest element. This in a simple dropping operation.
    Flow[SourceEvent]
      .conflate((lastEvent, newEvent) => newEvent)
}

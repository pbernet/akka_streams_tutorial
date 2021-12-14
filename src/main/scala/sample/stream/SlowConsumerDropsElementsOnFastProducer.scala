package sample.stream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.{DelayOverflowStrategy, ThrottleMode}

import java.time.{Instant, ZoneId, ZonedDateTime}
import scala.concurrent.duration._
import scala.util.Failure

case class SourceEvent(id: Integer)

case class DomainEvent(id: Integer, timeDate: ZonedDateTime)


/**
  * Inspired by:
  * https://github.com/DimaD/akka-streams-slow-consumer/blob/master/src/main/scala/Example.scala
  *
  * Doc:
  * https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/conflate.html
  * https://doc.akka.io/docs/akka/current/stream/stream-cookbook.html#dropping-elements
  *
  */
object SlowConsumerDropsElementsOnFastProducer extends App {
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val fastSource: Source[SourceEvent, NotUsed] =
    Source(1 to 500)
      .throttle(10, 1.second, 1, ThrottleMode.shaping)
      .map { i =>
        println(s"Producing event: $i")
        SourceEvent(i)
      }

  val droppyStream: Flow[SourceEvent, SourceEvent, NotUsed] =
  //Conflate is "rate aware", it combines/aggregates elements from upstream while downstream backpressures
  //The reducer function here takes the freshest element. This in a simple dropping operation.
    Flow[SourceEvent]
      .conflate((lastEvent, newEvent) => newEvent)

  val enrichWithTimestamp: Flow[SourceEvent, DomainEvent, NotUsed] =
    Flow[SourceEvent]
      .map { e =>
        val instant = Instant.ofEpochMilli(System.currentTimeMillis())
        val zonedDateTimeUTC: ZonedDateTime = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"))
        DomainEvent(e.id, zonedDateTimeUTC)
      }

  val terminationHook: Flow[DomainEvent, DomainEvent, Unit] = Flow[DomainEvent]
    .watchTermination() { (_, done) =>
      done.onComplete {
        case Failure(err) => println(s"Flow failed: $err")
        case _ => system.terminate(); println(s"Flow terminated")
      }
    }

  val slowSink: Sink[DomainEvent, NotUsed] =
    Flow[DomainEvent]
      //.buffer(100, OverflowStrategy.backpressure)
      .delay(10.seconds, DelayOverflowStrategy.backpressure)
      .to(Sink.foreach(e => println(s"Reached Sink: $e")))

  fastSource
    .via(droppyStream)
    .via(enrichWithTimestamp)
    .via(terminationHook)
    .runWith(slowSink)
}

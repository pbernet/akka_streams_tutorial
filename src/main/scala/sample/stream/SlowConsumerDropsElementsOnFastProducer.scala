package sample.stream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import org.slf4j.{Logger, LoggerFactory}

import java.time.{Instant, ZoneId, ZonedDateTime}
import scala.concurrent.duration._

case class SourceEvent(id: String)

case class DomainEvent(id: String, dateTime: ZonedDateTime)

/**
  * Inspired by:
  * https://github.com/DimaD/akka-streams-slow-consumer/blob/master/src/main/scala/Example.scala
  * Additional example with SourceQueue instead of Source.
  *
  * Doc:
  * https://doc.akka.io/docs/akka/current/stream/stream-rate.html#understanding-conflate
  * https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/conflate.html
  * https://doc.akka.io/docs/akka/current/stream/stream-cookbook.html#dropping-elements
  * https://doc.akka.io/docs/akka/current/stream/stream-rate.html
  */
object SlowConsumerDropsElementsOnFastProducer extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  runWithSource()
  runWithSourceQueue()

  private def droppyStream: Flow[SourceEvent, SourceEvent, NotUsed] =
  // Conflate is "rate aware", it combines/aggregates elements from upstream while downstream backpressures
  // The aggregate function returns the freshest element (= newEvent)
  // This in a simple dropping operation, hence the name droppyStream
    Flow[SourceEvent]
      .conflate((lastEvent, newEvent) => newEvent)

  private def enrichWithTimestamp: Flow[SourceEvent, DomainEvent, NotUsed] =
    Flow[SourceEvent]
      .map { e =>
        val instant = Instant.ofEpochMilli(System.currentTimeMillis())
        val zonedDateTimeUTC: ZonedDateTime = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"))
        DomainEvent(e.id, zonedDateTimeUTC)
      }

  private def slowSink: Sink[DomainEvent, NotUsed] =
    Flow[DomainEvent]
      // The internal buffer in the delay operator has a default capacity of 16
      // Adding an inputBuffer AFTER the operator allows to control the buffer size
      .delay(2.seconds, DelayOverflowStrategy.backpressure)
      .addAttributes(Attributes.inputBuffer(initial = 1, max = 1))
      .to(Sink.foreach(e => logger.info(s"Reached Sink: $e")))


  private def runWithSource() = {
    val fastSource: Source[SourceEvent, NotUsed] =
      Source(1 to 100)
        .throttle(10, 1.second, 1, ThrottleMode.shaping)
        .map { i =>
          val event = SourceEvent(s"1-$i")
          logger.info(s"Producing: $event")
          event
        }

    fastSource
      // If you comment this out, you see backpressure "all the way"
      // thus all events will reach the sink, eventually
      .via(droppyStream)
      .via(enrichWithTimestamp)
      .runWith(slowSink)
  }

  private def runWithSourceQueue(): Unit = {
    val fastSourceQueue = Source
      // Changing the buffer size has an effect
      .queue(10, OverflowStrategy.backpressure)
      // If you comment this out, you see backpressure "all the way"
      // thus all events will reach the sink, eventually
      .via(droppyStream)
      .via(enrichWithTimestamp)
      .to(slowSink)
      .run()

    def offerToSourceQueue(each: SourceEvent) = {
      fastSourceQueue.offer(each).map {
        case QueueOfferResult.Enqueued => logger.info(s"enqueued $each")
        case QueueOfferResult.Dropped => logger.info(s"dropped $each")
        case QueueOfferResult.Failure(ex) => logger.info(s"Offer failed: $ex")
        case QueueOfferResult.QueueClosed => logger.info("Source Queue closed")
      }
    }

    Source(1 to 100)
      .throttle(10, 1.second, 1, ThrottleMode.shaping)
      // If we parallelize here, we get a hang, when running without droppyStream
      .mapAsync(1) { i: Int =>
        logger.info(s"Producing event: $i")
        offerToSourceQueue(SourceEvent(s"2-$i"))
      }
      .runWith(Sink.ignore)
  }
}

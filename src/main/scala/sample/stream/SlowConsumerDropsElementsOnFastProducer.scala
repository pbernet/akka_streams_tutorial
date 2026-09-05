package sample.stream

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.*
import org.slf4j.{Logger, LoggerFactory}

import java.time.{Instant, ZoneId, ZonedDateTime}
import scala.annotation.nowarn
import scala.concurrent.duration.*

case class SourceEvent(id: String)

case class DomainEvent(id: String, dateTime: ZonedDateTime)

/**
  * Inspired by:
  * https://github.com/DimaD/akka-streams-slow-consumer/blob/master/src/main/scala/Example.scala
  * Additional example with [[SourceQueue]] instead of [[Source]].
  *
  * Doc:
  * https://pekko.apache.org/docs/pekko/snapshot/stream/stream-rate.html#understanding-conflate
  * https://pekko.apache.org/docs/pekko/snapshot/stream/operators/Source-or-Flow/conflate.html
  * https://pekko.apache.org/docs/pekko/snapshot/stream/stream-cookbook.html#dropping-elements
  * https://pekko.apache.org/docs/pekko/snapshot/stream/stream-rate.html
  *
  * Note: The `Source.queue(bufferSize, overflowStrategy)` API demonstrated here is deprecated since Pekko 2.0.0.
  * See: https://pekko.apache.org/docs/pekko/snapshot/stream/operators/Source/queue.html
  */
object SlowConsumerDropsElementsOnFastProducer {
  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    implicit val system: ActorSystem = ActorSystem()

    import system.dispatcher

    runWithSource()
    runWithSourceQueue()

    def droppyStream: Flow[SourceEvent, SourceEvent, NotUsed] =
      // Conflate is "rate aware", it combines/aggregates elements from upstream while downstream backpressures
      // The aggregate function returns the freshest element (= newEvent)
      // This in a simple dropping operation, hence the name droppyStream
      Flow[SourceEvent]
        .conflate((lastEvent, newEvent) => newEvent)

    def enrichWithTimestamp: Flow[SourceEvent, DomainEvent, NotUsed] =
      Flow[SourceEvent]
        .map { e =>
          val instant = Instant.ofEpochMilli(System.currentTimeMillis())
          val zonedDateTimeUTC: ZonedDateTime = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"))
          DomainEvent(e.id, zonedDateTimeUTC)
        }

    def slowSink: Sink[DomainEvent, NotUsed] =
      Flow[DomainEvent]
        // The internal buffer in the delay operator has a default capacity of 16
        // Adding an inputBuffer AFTER the operator allows to control the buffer size
        .delay(2.seconds, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes.inputBuffer(initial = 1, max = 1))
        .to(Sink.foreach(e => logger.info(s"Reached Sink: $e")))


    def runWithSource() = {
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

    @nowarn("cat=deprecation")
    def runWithSourceQueue(): Unit = {
      val fastSourceQueue = Source
        // Changing the buffer size has an effect. This overload is intentionally retained as a deprecated API example.
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
        .mapAsync(1) { i =>
          logger.info(s"Producing event: $i")
          offerToSourceQueue(SourceEvent(s"2-$i"))
        }
        .runWith(Sink.ignore)
    }
  }
}

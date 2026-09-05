package sample.stream_shared_state

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{MergeHub, Sink, Source}
import org.slf4j.{Logger, LoggerFactory}

import java.time.format.DateTimeFormatter
import java.time.{Instant, OffsetDateTime, ZoneId}
import scala.collection.mutable
import scala.collection.parallel.CollectionConverters.*
import scala.concurrent.duration.*
import scala.util.Random

/**
  * Scalable version of [[WindowingExample]], which has just one producer
  * N parallel clients send events to the "windowingProcessorSink"
  * This example uses [[MergeHub]] so many independent producers can attach to one pipeline:
  *
  * Client 1 ─┐
  * Client 2 ─┼──▶ MergeHub ──▶ windowing pipeline
  * Client N ─┘
  *
  * Currently, there is no deduplication of events within a window (kept in [[TreeSet]])
  */
object WindowingExampleScale {
  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    implicit val system: ActorSystem = ActorSystem()

    val maxSubstreams = 64
    val random = new Random()

    val delayFactor = 8
    val acceptedMaxDelay = 6.seconds.toMillis // Lower value leads to dropping of events

    implicit val ordering: Ordering[MyEvent] = (x: MyEvent, y: MyEvent) => {
      if (x.timestamp < y.timestamp) -1
      else if (x.timestamp > y.timestamp) 1
      else 0
    }

    val bufferSize = 1000
    val numberOfPublishingClients = 100

    lazy val windowingProcessorSink: Sink[MyEvent, NotUsed] =
      MergeHub
        .source[MyEvent](perProducerBufferSize = bufferSize)
        .statefulMap(
          // state creation function
          () => new CommandGenerator())(
          // mapping function
          (generator, nextElem) => (generator, generator.forEvent(nextElem)),
          // cleanup function
          generator => Some(generator.forEvent(createEvent(0))))
        .mapConcat(identity) // flatten
        .groupBy(maxSubstreams, command => command.w, allowClosedSubstreamRecreation = true)
        .takeWhile(!_.isInstanceOf[CloseWindow])
        .fold(AggregateEventData(Window(0L, 0L), mutable.TreeSet[MyEvent]())) {
          case (_, OpenWindow(window)) => AggregateEventData(w = window, new mutable.TreeSet[MyEvent])
          // always filtered out by takeWhile above
          case (agg, CloseWindow(_)) => agg
          case (agg, AddToWindow(ev, _)) => agg.copy(events = agg.events += ev)
        }
        .async
        .wireTap(each => logger.info(each.toString))
        .mergeSubstreams
        .to(Sink.ignore)
        .run()

    (1 to numberOfPublishingClients).par.foreach(each => client(each))

    def client(id: Int) = {
      logger.info(s"Starting client with id: $id")
      Source
        .tick(0.seconds, 10.millis, "")
        .map(_ => createEvent(id))
        .runWith(windowingProcessorSink)
    }

    def createEvent(id: Int) = {
      val now = System.currentTimeMillis()
      val delay = random.nextInt(delayFactor)
      val myEvent = MyEvent(now - delay * 1000L, id)
      logger.debug(s"$myEvent")
      myEvent
    }

    case class MyEvent(timestamp: Long, id: Int) {
      override def toString =
        s"Event: ${tsToString(timestamp)} source: $id"
    }

    case class Window(startTs: Long, stopTs: Long) {
      override def toString =
        s"Window from: ${tsToString(startTs)} to: ${tsToString(stopTs)}"
    }

    object Window {
      val WindowLength: Long = 10.seconds.toMillis
      val WindowStep: Long = 10.seconds.toMillis
      val WindowsPerEvent: Int = (WindowLength / WindowStep).toInt

      def windowsFor(ts: Long): Set[Window] = {
        val firstWindowStart = ts - ts % WindowStep - WindowLength + WindowStep
        (for (i <- 0 until WindowsPerEvent) yield
          Window(firstWindowStart + i * WindowStep,
            firstWindowStart + i * WindowStep + WindowLength)
          ).toSet
      }
    }

    sealed trait WindowCommand {
      def w: Window
    }

    case class OpenWindow(w: Window) extends WindowCommand

    case class CloseWindow(w: Window) extends WindowCommand

    case class AddToWindow(ev: MyEvent, w: Window) extends WindowCommand

    class CommandGenerator {
      private var watermark = 0L
      private val openWindows = mutable.Set[Window]()

      def forEvent(ev: MyEvent): List[WindowCommand] = {
        // watermark: the timestamp of the *newest* event minus acceptedMaxDelay
        watermark = math.max(watermark, ev.timestamp - acceptedMaxDelay)
        if (ev.timestamp < watermark) {
          logger.debug(s"Dropping event: $ev, watermark is at: ${tsToString(watermark)}")
          Nil
        } else {
          val eventWindows = Window.windowsFor(ev.timestamp)

          val closeCommands = openWindows.flatMap { ow =>
            if (ow.stopTs < watermark) {
              logger.info(s"Close $ow with watermark: ${tsToString(watermark)}")
              openWindows.remove(ow)
              Some(CloseWindow(ow))
            } else None
          }

          val openCommands = eventWindows.flatMap { w =>
            if (!openWindows.contains(w)) {
              logger.info(s"Open new $w")
              openWindows.add(w)
              Some(OpenWindow(w))
            } else None
          }

          val addCommands = eventWindows.map(w => AddToWindow(ev, w))

          openCommands.toList ++ closeCommands.toList ++ addCommands.toList
        }
      }
    }

    case class AggregateEventData(w: Window, events: mutable.TreeSet[MyEvent]) {
      override def toString =
        s"From: ${tsToString(w.startTs)} to: ${tsToString(w.stopTs)}, there were: ${events.size} events. Details: $events"
    }

    def tsToString(ts: Long) = OffsetDateTime
      .ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault())
      .toLocalTime
      .format(DateTimeFormatter.ofPattern("HH:mm:ss"))
  }
}

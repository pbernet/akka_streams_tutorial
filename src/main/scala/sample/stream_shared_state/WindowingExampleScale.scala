package sample.stream_shared_state

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source, SourceQueueWithComplete}
import org.apache.pekko.stream.{OverflowStrategy, QueueOfferResult}
import org.slf4j.{Logger, LoggerFactory}

import java.time.format.DateTimeFormatter
import java.time.{Instant, OffsetDateTime, ZoneId}
import scala.collection.mutable
import scala.collection.parallel.CollectionConverters._
import scala.concurrent.duration._
import scala.util.Random

/**
  * Scalable version of [[WindowingExample]]
  * N parallel clients send events to the "windowingProcessor" (Impl. with SourceQueue)
  * Currently there is no deduplication of events within a window (kept in TreeSet)
  *
  */
object WindowingExampleScale extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val maxSubstreams = 64
  val random = new Random()

  val delayFactor = 8
  val acceptedMaxDelay = 6.seconds.toMillis // Lower value leads to dropping of events

  implicit val ordering: Ordering[MyEvent] = new Ordering[MyEvent] {
    def compare(x: MyEvent, y: MyEvent): Int = {
      if (x.timestamp < y.timestamp) -1
      else if (x.timestamp > y.timestamp) 1
      else 0
    }
  }

  val bufferSize = 1000
  val maxConcurrentOffers = 100
  val numberOfPublishingClients = 100

  val windowingProcessorSourceQueue: SourceQueueWithComplete[MyEvent] =
    Source
      .queue[MyEvent](bufferSize, OverflowStrategy.backpressure, maxConcurrentOffers)
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
      .toMat(Sink.ignore)(Keep.left)
      .run()

  (1 to numberOfPublishingClients).par.foreach(each => client(each))

  def client(id: Int) = {
    logger.info(s"Starting client with id: $id")
    Source
      .tick(0.seconds, 10.millis, "")
      .map(_ => createEvent(id))
      .map(offerToSourceQueue)
      .runWith(Sink.ignore)
  }

  private def createEvent(id: Int) = {
    val now = System.currentTimeMillis()
    val delay = random.nextInt(delayFactor)
    val myEvent = MyEvent(now - delay * 1000L, id)
    logger.debug(s"$myEvent")
    myEvent
  }

  private def offerToSourceQueue(each: MyEvent) = {
    windowingProcessorSourceQueue.offer(each).map {
      case QueueOfferResult.Enqueued => logger.debug(s"enqueued $each")
      case QueueOfferResult.Dropped => logger.info(s"dropped $each")
      case QueueOfferResult.Failure(ex) => logger.warn(s"Offer failed: $ex")
      case QueueOfferResult.QueueClosed => logger.info("Source Queue closed")
    }
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
    val WindowLength = 10.seconds.toMillis
    val WindowStep = 10.seconds.toMillis
    val WindowsPerEvent = (WindowLength / WindowStep).toInt

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
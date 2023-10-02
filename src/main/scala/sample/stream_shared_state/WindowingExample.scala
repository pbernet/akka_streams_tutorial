package sample.stream_shared_state

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Source

import java.time.{Instant, OffsetDateTime, ZoneId}
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.Random

/**
  * Stolen from, inspired by:
  * https://gist.github.com/adamw/3803e2361daae5bdc0ba097a60f2d554
  *
  * Doc:
  * https://softwaremill.com/windowing-data-in-akka-streams
  *
  * Thanks to the generic implementation this example may be run as:
  * 1) Time-based sliding windows, eg with:
  *     - WindowLength set to: 10 seconds
  *     - WindowStep   set to: 1 second
  *
  * 2) Time-based tumbling windows, eg with:
  *     - WindowLength set to: 10 seconds
  *     - WindowStep   set to: 10 seconds
  *
  * Remarks:
  *  - 1) is documented in the blog, 2) is default here
  *  - The additional param allowClosedSubstreamRecreation on groupBy
  *    allows reusing closed substreams and thus avoids potential memory issues
  *  - Nice paper on watermarks: http://vldb.org/pvldb/vol14/p3135-begoli.pdf
  */
object WindowingExample extends App {
  implicit val system: ActorSystem = ActorSystem()

  val maxSubstreams = 64
  val random = new Random()

  val delayFactor = 8
  val acceptedMaxDelay = 4.seconds.toMillis // Lower value leads to dropping of events

  implicit val ordering = new Ordering[MyEvent]{
    def compare(x: MyEvent, y: MyEvent): Int = {
      if (x.timestamp < y.timestamp) -1
      else if (x.timestamp > y.timestamp) 1
      else 0
    }
  }

  Source
    .tick(0.seconds, 1.second, "")
    .map(_ => createEvent())
    .statefulMapConcat { () =>
      val generator = new CommandGenerator()
      ev => generator.forEvent(ev)
    }
    .groupBy(maxSubstreams, command => command.w, allowClosedSubstreamRecreation = true)
    .takeWhile(!_.isInstanceOf[CloseWindow])
    .fold(AggregateEventData(Window(0L, 0L), mutable.TreeSet[MyEvent]())) {
      case (_, OpenWindow(window)) => AggregateEventData(w = window, new mutable.TreeSet[MyEvent])
      // always filtered out by takeWhile above
      case (agg, CloseWindow(_)) => agg
      case (agg, AddToWindow(ev, _)) => agg.copy(events = agg.events += ev)
    }
    .async
    .mergeSubstreams
    .runForeach(println(_))

  private def createEvent() = {
    val now = System.currentTimeMillis()
    val delay = random.nextInt(delayFactor)
    val myEvent = MyEvent(now - delay * 1000L)
    println(s"$myEvent")
    myEvent
  }

  case class MyEvent(timestamp: Long) {
    override def toString =
      s"Event: ${tsToString(timestamp)}"
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
        println(s"Dropping $ev, watermark is at: ${tsToString(watermark)}")
        Nil
      } else {
        val eventWindows = Window.windowsFor(ev.timestamp)

        val closeCommands = openWindows.flatMap { ow =>
          if (ow.stopTs < watermark) {
            println(s"Close $ow")
            openWindows.remove(ow)
            Some(CloseWindow(ow))
          } else None
        }

        val openCommands = eventWindows.flatMap { w =>
          if (!openWindows.contains(w)) {
            println(s"Open new $w")
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
    .toString
}
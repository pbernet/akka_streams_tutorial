package akkastreams

/**
  * Windowing sample taken from:
  * https://gist.github.com/adamw/3803e2361daae5bdc0ba097a60f2d554
  *
  * Doc:
  * https://softwaremill.com/windowing-data-in-akka-streams
  *
  * Probably Apache Flink is more suited for stateful stream processing - see:
  * https://ci.apache.org/projects/flink/flink-docs-release-1.3/concepts/programming-model.html#windows
  * https://flink.apache.org/news/2015/12/04/Introducing-windows.html
  */
import java.time._

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

object WindowingExample {
  def main(args: Array[String]): Unit = {
    implicit val as = ActorSystem("WindowingExample")
    implicit val mat = ActorMaterializer()

    val random = new Random()

    val f = Source
      .tick(0.seconds, 1.second, "")
      .map { _ =>
        val now = System.currentTimeMillis()
        val delay = random.nextInt(8)
        MyEvent(now - delay * 1000L)
      }
      .statefulMapConcat { () =>
        val generator = new CommandGenerator()
        ev => generator.forEvent(ev)
      }
      .groupBy(64, command => command.w)
      .takeWhile(!_.isInstanceOf[CloseWindow])
      .fold(AggregateEventData((0L, 0L), 0)) {
        case (agg, OpenWindow(window)) => agg.copy(w = window)
        // always filtered out by takeWhile
        case (agg, CloseWindow(_))     => agg
        case (agg, AddToWindow(ev, _)) => agg.copy(eventCount = agg.eventCount+1)
      }
      .async
      .mergeSubstreams
      .runForeach { agg =>
        println(agg.toString)
      }

    try Await.result(f, 60.seconds)
    finally as.terminate()
  }

  case class MyEvent(timestamp: Long)

  type Window = (Long, Long)
  object Window {
    val WindowLength    = 10.seconds.toMillis
    val WindowStep      =  1.second .toMillis
    val WindowsPerEvent = (WindowLength / WindowStep).toInt

    def windowsFor(ts: Long): Set[Window] = {
      val firstWindowStart = ts - ts % WindowStep - WindowLength + WindowStep
      (for (i <- 0 until WindowsPerEvent) yield
        (firstWindowStart + i * WindowStep,
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
    private val MaxDelay = 5.seconds.toMillis
    private var watermark = 0L
    private val openWindows = mutable.Set[Window]()

    def forEvent(ev: MyEvent): List[WindowCommand] = {
      watermark = math.max(watermark, ev.timestamp - MaxDelay)
      if (ev.timestamp < watermark) {
        println(s"Dropping event with timestamp: ${tsToString(ev.timestamp)}")
        Nil
      } else {
        val eventWindows = Window.windowsFor(ev.timestamp)

        val closeCommands = openWindows.flatMap { ow =>
          if (!eventWindows.contains(ow) && ow._2 < watermark) {
            openWindows.remove(ow)
            Some(CloseWindow(ow))
          } else None
        }

        val openCommands = eventWindows.flatMap { w =>
          if (!openWindows.contains(w)) {
            openWindows.add(w)
            Some(OpenWindow(w))
          } else None
        }

        val addCommands = eventWindows.map(w => AddToWindow(ev, w))

        openCommands.toList ++ closeCommands.toList ++ addCommands.toList
      }
    }
  }

  case class AggregateEventData(w: Window, eventCount: Int) {
    override def toString =
      s"Between ${tsToString(w._1)} and ${tsToString(w._2)}, there were $eventCount events."
  }

  def tsToString(ts: Long) = OffsetDateTime
    .ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault())
    .toLocalTime
    .toString
}
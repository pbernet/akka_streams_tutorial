package sample.graphstage

import org.apache.pekko.stream.stage._
import org.apache.pekko.stream.{Attributes, FlowShape, Inlet, Outlet}

import scala.concurrent.duration.{DurationInt, FiniteDuration}

final class Pulse[T](interval: FiniteDuration, initiallyOpen: Boolean = false)
  extends GraphStage[FlowShape[T, T]] {

  val in = Inlet[T]("Pulse.in")
  val out = Outlet[T]("Pulse.out")
  val shape = FlowShape(in, out)

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {

      setHandlers(in, out, this)

      override def preStart(): Unit = if (!initiallyOpen) startPulsing()
      override def onPush(): Unit = if (isAvailable(out)) push(out, grab(in))
      override def onPull(): Unit = if (!pulsing) {
        pull(in)
        startPulsing()
      }

      override protected def onTimer(timerKey: Any): Unit = {
        if (isAvailable(out) && !isClosed(in) && !hasBeenPulled(in)) pull(in)
      }

      private def startPulsing() = {
        pulsing = true
        scheduleWithFixedDelay("PulseTimer", 100.millis, interval)
      }
      private var pulsing = false
    }

  override def toString = "Pulse"
}
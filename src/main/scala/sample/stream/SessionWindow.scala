package sample.stream

import akka.actor.ActorSystem
import akka.stream.scaladsl._
import sample.stream_shared_state.SplitWhen

import scala.collection.immutable.Iterable
import scala.util.{Failure, Success}

/**
  * Split events into session windows using event time
  *
  * Similar to: [[SplitWhen]]
  *
  */
object SessionWindow extends App {
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val maxGap = 5 // between session windows

  case class Event[T](timestamp: Long, data: T)

  private def hasGap(maxGap: Long): () => Seq[Event[String]] => Iterable[(Event[String], Boolean)] = {
    () => {
      slidingElements => {
        if (slidingElements.size == 2) {
          val current = slidingElements.head.timestamp
          val next = slidingElements.tail.head.timestamp
          val gap = next - current
          List((slidingElements.head, gap >= maxGap))
        } else {
          List((slidingElements.head, false))
        }
      }
    }
  }

  private def sessionWindowEvent(maxGap: Long) =
    Flow[Event[String]].sliding(2) // allows to compare this element with the next element
      .statefulMapConcat(hasGap(maxGap)) // stateful decision
      .splitAfter(_._2) // split when gap has been reached
      .map(_._1) // proceed with payload
      //.wireTap(each => println(each))
      .fold(Vector.empty[Event[String]])(_ :+ _)
      .mergeSubstreams


  val dummy = Source.single(Event(0, "DummyLastEvent"))

  val input = Source(List(
    Event(1, "A"),
    Event(7, "B"), Event(8, "C"),
    Event(15, "D"), Event(16, "E"),
    Event(25, "F"), Event(26, "G"), Event(26, "H"),
    Event(32, "H")
  )) ++ dummy

  val result = input
    .via(sessionWindowEvent(maxGap))
    .runWith(Sink.seq)

  result.onComplete {
    case Success(sessions) => println(s"Sessions: ${sessions.mkString}")
    case Failure(e) => e.printStackTrace()
  }
}

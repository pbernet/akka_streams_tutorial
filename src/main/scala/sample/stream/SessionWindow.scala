package sample.stream

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl._

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Split ordered events into `session windows`
  * using the event timestamp
  *
  * Similar to: [[tools.SubtitleTranslator]]
  *
  */
object SessionWindow extends App {
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val maxGap = 5 // between session windows

  case class Event[T](timestamp: Long, data: T)

  private def groupBySessionWindow(maxGap: Long) =
    Flow[Event[String]].statefulMap(() => List.empty[Event[String]])(
      (stateList, nextElem) => {
        val newStateList = stateList :+ nextElem
        val lastElem = if (stateList.isEmpty) nextElem else stateList.reverse.head
        val calcGap = nextElem.timestamp - lastElem.timestamp
        if (calcGap < maxGap) {
          // (list for next iteration, list of output elements)
          (newStateList, Nil)
        }
        else {
          // (list for next iteration, list of output elements)
          (List(nextElem), stateList)
        }
      },
      // Cleanup function, we return the last stateList
      stateList => Some(stateList))

  val input = Source(List(
    Event(1, "A"),
    Event(7, "B"), Event(8, "C"),
    Event(15, "D"), Event(16, "E"), Event(18, "F"),
    Event(25, "G"), Event(26, "H"), Event(26, "I"), Event(28, "J"),
    Event(32, "K"),
    Event(42, "L"), Event(43, "M")
  ))

  val result: Future[Seq[List[Event[String]]]] = input
    .via(groupBySessionWindow(maxGap))
    .filterNot(each => each.isEmpty)
    .runWith(Sink.seq)

  result.onComplete {
    case Success(sessions) =>
      sessions.foreach(sessionWindow => println(s"Session window with events: ${sessionWindow.mkString}"))
      system.terminate()
    case Failure(e) => e.printStackTrace()
      system.terminate()
  }
}

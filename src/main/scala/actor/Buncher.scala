package actor

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}

import scala.collection.immutable
import scala.concurrent.duration.*

/**
  * This akka typed actor FSM example demonstrates how to:
  *  - Model states using different behaviors
  *  - Model storing data at each state by representing the behavior as a method
  *  - Implement state timeouts
  *
  * Doc:
  * https://doc.akka.io/docs/akka/current/typed/fsm.html
  */
object Buncher {
  def main(args: Array[String]): Unit = {
    val _ = system
  }

  val root = Behaviors.setup[Nothing] { context =>
    val buncherActor = context.spawn(Buncher(), "buncherActor")
    val targetActor = context.spawn(TargetActor(), "targetActor")

    buncherActor ! SetTarget(targetActor)
    buncherActor ! Queue(42)
    buncherActor ! Queue("abc")
    Thread.sleep(2000) //Provoke flushing by Timeout
    buncherActor ! Queue(43)
    buncherActor ! Queue(44)
    buncherActor ! Flush
    buncherActor ! Queue(45)

    Behaviors.empty
  }

  lazy val system: ActorSystem[Nothing] = ActorSystem[Nothing](root, "Buncher")


  // received events: the type of the message this Actor supports
  sealed trait Event
  final case class SetTarget(ref: ActorRef[Batch]) extends Event
  final case class Queue(obj: Any) extends Event
  case object Flush extends Event
  private case object Timeout extends Event

  // sent event: to targetActor
  final case class Batch(obj: immutable.Seq[Any])

  // internal data
  sealed trait Data
  case object Uninitialized extends Data
  final case class Todo(target: ActorRef[Batch], queue: immutable.Seq[Any]) extends Data


  // initial state
  def apply(): Behavior[Event] = idle(Uninitialized)

  // states of the FSM represented as behaviors

  private def idle(data: Data): Behavior[Event] = Behaviors.receiveMessage[Event] { message =>
    (message, data) match {
      case (SetTarget(ref), Uninitialized) =>
        // action: add obj to queue
        // transition: to the state idle
        idle(Todo(ref, Vector.empty))
      case (Queue(obj), t @ Todo(_, queue)) =>
        // action: add obj to vector
        // transition: to the state active
        active(t.copy(queue = queue :+ obj))
      case _ =>
        Behaviors.unhandled
    }
  }

  private def active(data: Todo): Behavior[Event] =
    Behaviors.withTimers[Event] { timers =>
      // send msg Timeout after 1 sec (instead of FSM state timeout)
      timers.startSingleTimer(Timeout, 1.second)
      Behaviors.receiveMessagePartial {
        case Flush | Timeout =>
          // action: send queue to targetActor
          // transition: to the state idle with empty queue
          data.target ! Batch(data.queue)
          idle(data.copy(queue = Vector.empty))
        case Queue(obj) =>
          // action: add obj to queue
          // transition: to the state active
          active(data.copy(queue = data.queue :+ obj))
      }
    }
}

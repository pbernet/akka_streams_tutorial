package actor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

object BlockingActor {
  def apply(): Behavior[Int] =
    Behaviors.receive { (context, i) =>
      context.log.info(s"Started: $i by ${Thread.currentThread().getName}")
      //block for 5 seconds, representing blocking I/O, etc
      Thread.sleep(5000)
      context.log.info(s"Finished: $i")
      Behaviors.same
    }
}
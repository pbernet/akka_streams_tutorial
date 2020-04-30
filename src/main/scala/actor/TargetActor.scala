package actor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

object TargetActor {
  def apply(): Behavior[Buncher.Batch] =
    Behaviors.receive { (ctx, i) =>
      ctx.log.info(i.toString)
      Behaviors.same
    }
}
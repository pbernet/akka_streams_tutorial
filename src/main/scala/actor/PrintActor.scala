package actor

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

object PrintActor {
  def apply(): Behavior[Integer] =
    Behaviors.receive { (ctx, i) =>
      ctx.log.info(i.toString)
      Behaviors.same
    }
}
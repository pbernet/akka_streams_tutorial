package actor

import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors

object PrintActor {
  def apply(): Behavior[Integer] =
    Behaviors.receive { (ctx, i) =>
      ctx.log.info(s"Finished: $i by ${Thread.currentThread().getName}")
      Behaviors.same
    }
}
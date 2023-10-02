package actor

import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.Behaviors

object TargetActor {
  def apply(): Behavior[Buncher.Batch] =
    Behaviors.receive { (ctx, i) =>
      ctx.log.info(i.toString)
      Behaviors.same
    }
}
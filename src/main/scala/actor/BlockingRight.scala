package actor

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorSystem, DispatcherSelector}

/**
  * If blocking (e.g. by an external resource) is required,
  * a custom dispatcher (see application.conf) avoids
  * thread starvation of the default dispatcher
  *
  * Stolen from:
  * https://github.com/raboof/akka-blocking-dispatcher
  *
  * See [[sample.stream.WaitForFlowsToComplete]] for use of custom dispatcher in a stream
  *
  */
object BlockingRight {
  def main(args: Array[String]): Unit = {
    val _ = system
  }

  val root = Behaviors.setup[Nothing] { context =>
    (1 to 50).foreach { i =>
      // non blocking actor running on default-dispatcher
      context.spawn(PrintActor(), s"nonblocking-$i") ! i
      // blocking actor running on custom-dispatcher
      context.spawn(
        BlockingActor(),
        s"blocking-$i",
        DispatcherSelector.fromConfig("custom-dispatcher-for-blocking")
      ) ! i
    }

    Behaviors.empty
  }

  lazy val system: ActorSystem[Nothing] = ActorSystem[Nothing](root, "BlockingRight")
}

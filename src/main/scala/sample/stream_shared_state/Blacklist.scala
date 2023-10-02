package sample.stream_shared_state

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream._
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.stream.stage._

import scala.concurrent.duration._
import scala.language.reflectiveCalls

/**
  * Source -> Flow(Blacklist) -> Sink
  * Inject shared state (eg Blacklist) from outside the flow execution
  *
  * Implementation doc:
  * https://doc.akka.io/docs/akka/current/stream/stream-customize.html#custom-materialized-values
  *
  * Similar to [[ParametrizedFlow]]
  *
  * Sample Implementation of discussion:
  * https://discuss.lightbend.com/t/the-idiomatic-way-to-manage-shared-state-with-akka-streams/2552
  */

object Blacklist extends App {
  implicit val system: ActorSystem = ActorSystem()

  val initBlacklist = Set.empty[String]

  val service: StateService[Set[String]] =
    Source.repeat("yes")
      .throttle(1, 1.second, 10, ThrottleMode.shaping)
      .viaMat(new ZipWithState(initBlacklist))(Keep.right)
      .filterNot { case (blacklist: Set[String], elem: String) => blacklist(elem) }
      .to(Sink.foreach(each => println(each._2)))
      .run()

  println("Starting with empty blacklist on a list of 'yes' elements -> elements are passing")

  Thread.sleep(2000)
  println("Inject new blacklist with value: 'yes' -> elements are filtered")
  service.update(Set("yes"))

  Thread.sleep(5000)
  println("Inject new blacklist with value: 'no' -> elements are passing again")
  service.update(Set("no"))
}


trait StateService[A] {
  def update(state: A): Unit
}

class StateServiceCallback[A](callback: AsyncCallback[A]) extends StateService[A] {
  override def update(state: A): Unit = callback.invoke(state)
}

class ZipWithState[S, I](initState: S) extends GraphStageWithMaterializedValue[FlowShape[I, (S, I)], StateService[S]] {
  val in = Inlet[I]("ZipWithState.in")
  val out = Outlet[(S, I)]("ZipWithState.out")

  override val shape: FlowShape[I, (S, I)] = FlowShape.of(in, out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, StateService[S]) = {
    val logic = new GraphStageLogic(shape) {
      private[this] var state: S = initState
      val updateStateCallback: AsyncCallback[S] =
        getAsyncCallback[S] {
          state = _
        }

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          push(out, (state, grab(in)))
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })
    }

    (logic, new StateServiceCallback(logic.updateStateCallback))
  }
}
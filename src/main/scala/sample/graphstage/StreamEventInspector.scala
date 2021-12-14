package sample.graphstage

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Example of an atomic operator created with GraphStage, which may be used to wire tap a stream.
  * Inspired by: https://gist.github.com/hochgi/cc354f9b80ca427a4f4d7313c78e4350
  *
  * From the doc:
  * https://doc.akka.io/docs/akka/current/stream/stream-customize.html#custom-processing-with-graphstage
  *
  * A GraphStage can be used to create arbitrary atomic operators with any number of input or output ports.
  * GraphStages are atomic and allow state to be maintained inside it in a safe way.
  * GraphStage is a counterpart of the GraphDSL.create() method which creates new stream processing operators by composing others.
  *
  * Hooks:
  * @param onUpstreamFinishInspection
  * @param onUpstreamFailureInspection
  * @param onDownstreamFinishInspection
  * @param onPushInspection
  * @param onPullInspection
  * @tparam Elem
  */
class StreamEventInspector[Elem](onUpstreamFinishInspection:   ()        => Unit = () => {},
                                 onUpstreamFailureInspection:  Throwable => Unit = _  => {},
                                 onDownstreamFinishInspection: Throwable => Unit = _  => {},
                                 onPushInspection:             Elem      => Unit = (_: Elem)  => {},
                                 onPullInspection:             ()        => Unit = () => {}
                                ) extends GraphStage[FlowShape[Elem, Elem]] {

  private val in = Inlet[Elem]("StreamEventInspector.in")
  private val out = Outlet[Elem]("StreamEventInspector.out")

  override val shape: FlowShape[Elem, Elem] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    setHandler(
      in,
      new InHandler {
        override def onPush(): Unit = {
          val elem = grab(in)
          onPushInspection(elem)
          push(out, elem)
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          onUpstreamFailureInspection(ex)
          super.onUpstreamFailure(ex)
        }

        override def onUpstreamFinish(): Unit = {
          onUpstreamFinishInspection()
          super.onUpstreamFinish()
        }
      }
    )

    setHandler(
      out,
      new OutHandler {
        override def onPull(): Unit = {
          onPullInspection()
          pull(in)
        }

        override def onDownstreamFinish(cause: Throwable): Unit = {
          onDownstreamFinishInspection(cause)
          super.onDownstreamFinish(cause: Throwable)
        }
      }
    )
  }
}

object StreamEventInspector {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def apply[T](context: String, printElem: T => String): StreamEventInspector[T] = {
    val ctx = "[" + context + "] "
    new StreamEventInspector[T](
      () => logger.info(ctx + "upstream completed"),
      ex => logger.error(ctx + "upstream failure", ex),
      ex => logger.error(ctx + "downstream completed", ex),
      el => logger.info(ctx + printElem(el)),
      () => logger.info(ctx + "downstream pulled")
    )
  }
}
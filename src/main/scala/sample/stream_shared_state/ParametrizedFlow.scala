package sample.stream_shared_state

import org.apache.pekko.Done
import org.apache.pekko.actor.{ActorRef, ActorSystem, Cancellable}
import org.apache.pekko.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source, Zip}
import org.apache.pekko.stream.{FlowShape, OverflowStrategy}

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.util.{Failure, Success}


/**
  * Source -> Flow(parameter) -> Sink
  * Inject parameter from outside the flow execution
  *
  * The basic idea is to Zip this parameter with the periodic (extrapolated) flow values
  * and then apply a user function
  *
  * Implementation is done with GraphDSL, Doc:
  * https://pekko.apache.org/docs/pekko/snapshot/stream/stream-graphs.html
  *
  * Similar to [[Blacklist]]
  *
  * Inspired by:
  * https://discuss.lightbend.com/t/how-to-configure-flow-on-the-fly/6554/2
  *
  */
object ParametrizedFlow {
  def main(args: Array[String]): Unit = {
    val service = ParameterizedFlowService

    Thread.sleep(5000)
    service.update(1.0)

    Thread.sleep(2000)
    service.update(1.5)
    Thread.sleep(2000)
    service.cancel()
    Thread.sleep(2000)

    println(service.result())
  }
}

object ParameterizedFlowService {
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  def update(element: Double): Unit = flow._1._2._1 ! element

  def cancel(): Boolean = flow._1._1.cancel()

  def result(): Future[Seq[Double]] = flow._2

  val fun: (Int, Double) => Double = (flowValue: Int, paramValue: Double) => flowValue * paramValue
  val flow: ((Cancellable, (ActorRef, Future[Done])), Future[immutable.Seq[Double]]) =
    Source.tick(0.seconds, 500.millis, 10)
      .viaMat(createParamFlow(1, OverflowStrategy.dropBuffer, 0.5)(fun))(Keep.both)
      .wireTap(x => println(x))
      .toMat(Sink.seq)(Keep.both)
      .run()

  val done: Future[Done] = flow._1._2._2
  terminateWhen(done)

  private def createParamFlow[A, P, O](bufferSize: Int, overflowStrategy: OverflowStrategy, initialParam: P)(fun: (A, P) => O) =
    Flow.fromGraph(GraphDSL.createGraph(
      Source.actorRef[P](
          completionMatcher = PartialFunction.empty,
          failureMatcher = PartialFunction.empty,
          bufferSize = bufferSize,
          overflowStrategy = overflowStrategy)
        .watchTermination(Keep.both)) { implicit builder =>
      queue =>
        import GraphDSL.Implicits.*
        val zip = builder.add(Zip[A, P]())
        // Interesting use of the extrapolate operator
        // based on https://pekko.apache.org/docs/pekko/snapshot/stream/stream-rate.html#understanding-extrapolate-and-expand
        val extra = builder.add(Flow[P].extrapolate(Iterator.continually(_), Some(initialParam)))
        val map = builder.add(Flow[(A, P)].map(r => fun(r._1, r._2)))

        queue ~> extra ~> zip.in1
        zip.out ~> map
        FlowShape(zip.in0, map.out)
    })

  private def terminateWhen(done: Future[?]): Unit = {
    done.onComplete {
      case Success(_) =>
        println("Flow Success. About to terminate...")
        system.terminate()
      case Failure(e) =>
        println(s"Flow Failure: $e. About to terminate...")
        system.terminate()
    }
  }
}

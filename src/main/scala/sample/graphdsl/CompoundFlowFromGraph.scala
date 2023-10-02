package sample.graphdsl

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, Sink, Source}
import org.apache.pekko.stream.{FlowShape, UniformFanInShape, UniformFanOutShape}

/**
  * A GraphDSL example, to assemble and start a compound of
  * parallel operations (= processorFlows), each executed async.
  *
  * A similar example with the Flow API operators: [[sample.stream.AsyncExecution]]
  *
  * Inspired by:
  * https://groups.google.com/forum/#!topic/akka-user/Dh8q7TcP2SI
  *
  */
object CompoundFlowFromGraph extends App {
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val processorFlow1: Flow[Int, Int, NotUsed] = Flow[Int].map(_ * 2).wireTap(each => println(s"Processed by Flow1: $each"))
  val processorFlow2: Flow[Int, Int, NotUsed] = Flow[Int].map(_ * 3).wireTap(each => println(s"Processed by Flow2: $each"))
  val processorFlows: Seq[Flow[Int, Int, NotUsed]] = List(processorFlow1, processorFlow2)

  // A way to scale one flow
  val parallelism = Runtime.getRuntime.availableProcessors
  val parallelFlows: Seq[Flow[Int, Int, NotUsed]] = (1 to parallelism).map(_ => processorFlow1)


  def compoundFlowFrom[T](indexFlows: Seq[Flow[T, T, NotUsed]]): Flow[T, T, NotUsed] = {
    require(indexFlows.nonEmpty, "Cannot create compound flow without any flows to combine")

    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import org.apache.pekko.stream.scaladsl.GraphDSL.Implicits._

      val broadcast: UniformFanOutShape[T, T] = b.add(Broadcast(indexFlows.size))
      val merge: UniformFanInShape[T, T] = b.add(Merge(indexFlows.size))

      indexFlows.foreach(each => broadcast ~> each.async ~> merge)

      FlowShape(broadcast.in, merge.out)
    })
  }

  val compoundFlow: Flow[Int, Int, NotUsed] = compoundFlowFrom(processorFlows)

  Source(1 to 100)
    .via(compoundFlow)
    .runWith(Sink.foreach(each => println(s"Reached sink: $each")))
    .onComplete(_ => system.terminate())
}
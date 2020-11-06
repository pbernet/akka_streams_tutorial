package sample.graphdsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, Sink, Source}
import akka.stream.{FlowShape, UniformFanInShape, UniformFanOutShape}

/**
  * A GraphDSL example, which shows the possibility to assemble and start a compound
  * from parallel operations (= processorFlow), each executed async.
  * This pattern may be used to run one processorFlow in parallel as well.
  *
  * The advantage to [[sample.stream.AsyncExecution]] is that here we have more flexibility
  * assembling the listOfFlows
  *
  * Inspired by:
  * https://groups.google.com/forum/#!topic/akka-user/Dh8q7TcP2SI
  *
  */
object CompoundFlowFromGraph extends App {
    implicit val system = ActorSystem("CompoundFlowFromGraph")
    implicit val ec = system.dispatcher

    val processorFlow1: Flow[Int, Int, NotUsed] = Flow[Int].map(_ * 2).wireTap(each => println(s"Processed by Flow1: $each"))
    val processorFlow2: Flow[Int, Int, NotUsed] = Flow[Int].map(_ * 3).wireTap(each => println(s"Processed by Flow2: $each"))
    val listOfFlows: Seq[Flow[Int, Int, NotUsed]] = List(processorFlow1, processorFlow2)

    val parallelism = Runtime.getRuntime.availableProcessors
    val listOfFlow: Seq[Flow[Int, Int, NotUsed]] = (1 to parallelism).map(_ => processorFlow1)


    def compoundFlowFrom[T](indexFlows: Seq[Flow[T, T, NotUsed]]): Flow[T, T, NotUsed] = {
      require(indexFlows.nonEmpty, "Cannot create compound flow without any flows to combine")

      Flow.fromGraph(GraphDSL.create() { implicit b =>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val broadcast: UniformFanOutShape[T, T] = b.add(Broadcast(indexFlows.size))
        val merge: UniformFanInShape[T, T] = b.add(Merge(indexFlows.size))

        indexFlows.foreach(each => broadcast ~> each.async ~> merge)

        FlowShape(broadcast.in, merge.out)
      })
    }

    val compoundFlow: Flow[Int, Int, NotUsed] = compoundFlowFrom(listOfFlows)

    Source(1 to 100)
      .via(compoundFlow)
      .runWith(Sink.foreach(each => println(s"Reached sink: $each")))
      .onComplete(_ => system.terminate())
}
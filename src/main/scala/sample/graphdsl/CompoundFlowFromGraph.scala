package sample.graphdsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, Sink, Source}
import akka.stream.{FlowShape, UniformFanInShape, UniformFanOutShape}

/**
  * A GraphDSL example, which shows the possibility to inject parallel operations (= processorFlow)
  * on a compound flow.
  * However, this pattern can be used to run just one processorFlow in parallel as well.
  * Doing it this may may be more flexible than trying to go parallel
  * with operators, eg with groupBy / mergeSubstreams as in [[sample.stream.FlightDelayStreaming]]
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
    val listOfFlows = List(processorFlow1, processorFlow2)

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

    val compoundFlow = compoundFlowFrom(listOfFlows)

    Source(1 to 100)
      .via(compoundFlow)
      .runWith(Sink.foreach(each => println(s"Reached sink: $each")))
      .onComplete(_ => system.terminate())
}
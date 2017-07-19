package sample.graphDSL

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, Sink, Source}
import akka.stream.{ActorMaterializer, FlowShape, UniformFanInShape, UniformFanOutShape}

/**
  * A GraphDSL example, which shows the possibility to inject operations (= flows) on a
  * compound flow.
  *
  * Inspired by:
  * https://groups.google.com/forum/#!topic/akka-user/Dh8q7TcP2SI
  *
  */
object FlowFromGraph {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("FlowFromGraph")
    implicit val ec = system.dispatcher
    implicit val materializer = ActorMaterializer()

    val processorFlow1: Flow[Int, Int, NotUsed] = Flow[Int].map(_ * 2)
    val processorFlow2: Flow[Int, Int, NotUsed] = Flow[Int].map(_ * 3)
    val listOfFlows = List(processorFlow1, processorFlow2)

    def compoundFlowFrom[T](indexFlows: Seq[Flow[T, T, NotUsed]]): Flow[T, T, NotUsed] = {
      require(indexFlows.nonEmpty, "Cannot create compound flow without any flows to combine")

      Flow.fromGraph(GraphDSL.create() { implicit b =>
        import akka.stream.scaladsl.GraphDSL.Implicits._

        val broadcast: UniformFanOutShape[T, T] = b.add(Broadcast(indexFlows.size))
        val merge: UniformFanInShape[T, T] = b.add(Merge(indexFlows.size))

        indexFlows.foreach(broadcast ~> _ ~> merge)

        FlowShape(broadcast.in, merge.out)
      })
    }

    val compoundFlow = compoundFlowFrom(listOfFlows)

    Source(1 to 10)
      .via(compoundFlow)
      .runWith(Sink.foreach(println(_)))
      .onComplete(_ => system.terminate())
  }
}
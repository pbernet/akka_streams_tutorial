package sample.stream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, MergeHub, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, DelayOverflowStrategy}

import scala.concurrent.duration._

/**
  * Example taken from Doc:
  * http://doc.akka.io/docs/akka/current/scala/stream/stream-dynamic.html#dynamic-fan-in-and-fan-out-with-mergehub-broadcasthub-and-partitionhub
  *
  * This seems a more elegant solution for "sending messages to a stream from many threads"
  * than the solutions in PublishToSourceQueueFromManyThreads and PublishToSourceQueueFromManyThreads2
  *
  */
object MergeHubWithDynamicSources {
  implicit val system = ActorSystem()
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  def main(args: Array[String]): Unit = {

    val slowSink: Sink[String, NotUsed] =
      Flow[String]
        .delay(1.seconds, DelayOverflowStrategy.backpressure)
        .to(Sink.foreach(e => println(s"Reached Sink: $e")))

    // Attach a MergeHub Source to the consumer. This will materialize to a corresponding Sink
    val runnableGraph: RunnableGraph[Sink[String, NotUsed]] = MergeHub.source[String](perProducerBufferSize = 16).to(slowSink)

    // By running/materializing the graph we get back a Sink, and hence now have access to feed elements into it
    // This Sink can then be materialized any number of times, and every element that enters the Sink will be consumed by our consumer
    val toConsumer: Sink[String, NotUsed] = runnableGraph.run()

    def fastSource(index: Int, toConsumer: Sink[String, NotUsed]) = {
      val tickSource = Source.tick(100.millis, 100.millis, index).map(_.toString)
      tickSource.runWith(toConsumer)
    }

    // Feed first dynamic fan-in source into the hub
    Source.single("Hello MergeHub!").runWith(toConsumer)

    // Add more dynamic producer sources. If the consumer cannot keep up, then ALL of the producers are backpressured
    (1 to 100).par.foreach(each => fastSource(each, toConsumer))
  }
}

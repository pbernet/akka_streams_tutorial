package sample.stream

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.DelayOverflowStrategy
import org.apache.pekko.stream.scaladsl.{Flow, MergeHub, RunnableGraph, Sink, Source}

import scala.collection.parallel.CollectionConverters.*
import scala.concurrent.duration.*

/**
  * Inspired by:
  * http://doc.akka.io/docs/akka/current/scala/stream/stream-dynamic.html#dynamic-fan-in-and-fan-out-with-mergehub-broadcasthub-and-partitionhub
  *
  * Similar example: [[PublishToSourceQueueFromMultipleThreads]]
  *
  */
object MergeHubWithDynamicSources {
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem()

    val slowSinkConsumer: Sink[Seq[String], NotUsed] =
      Flow[Seq[String]]
        .delay(1.seconds, DelayOverflowStrategy.backpressure)
        .to(Sink.foreach(e => println(s"Reached Sink: $e")))

    // Attach a MergeHub Source to the consumer. This will materialize to a corresponding Sink
    val runnableGraph: RunnableGraph[Sink[String, NotUsed]] =
      MergeHub.source[String](perProducerBufferSize = 16)
        .groupedWithin(10, 2.seconds)
        .to(slowSinkConsumer)

    // By running/materializing the graph we get back a Sink, and hence are able to feed elements into it
    // This Sink can be materialized any number of times, and every element that enters the Sink reaches the slowSinkConsumer
    val toConsumer: Sink[String, NotUsed] = runnableGraph.run()

    def fastDynamicSource(sourceId: Int, toConsumer: Sink[String, NotUsed]) = {
      Source(1 to 10)
        .map { each => println(s"Produced: $sourceId.$each"); s"$sourceId.$each" }
        .runWith(toConsumer)
    }

    // Add dynamic producer sources
    // If the consumer cannot keep up, then ALL of the producers are backpressured
    (1 to 10).par.foreach(each => fastDynamicSource(each, toConsumer))
  }
}

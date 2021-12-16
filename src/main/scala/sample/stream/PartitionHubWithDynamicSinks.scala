package sample.stream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ThrottleMode
import akka.stream.scaladsl.{Keep, PartitionHub, RunnableGraph, Source}

import scala.concurrent.duration._

/**
  * Inspired by:
  * https://doc.akka.io/docs/akka/current/stream/stream-dynamic.html
  *
  * Partitioning functions:
  * - partitionRoundRobin:        Route to only one consumer (incl. slow consumers)
  * - partitionToFastestConsumer: Route to the fastest consumer (based on queueSize)
  *
  */
object PartitionHubWithDynamicSinks extends App {
  implicit val system: ActorSystem = ActorSystem()
  implicit val ec = system.dispatcher

  val producer = Source.tick(1.second, 100.millis, "message").zipWith(Source(1 to 100))((a, b) => s"$a-$b")

  // A new instance of the partitioner functions and its state is created for each materialization of the PartitionHub
  def partitionRoundRobin(): (PartitionHub.ConsumerInfo, String) => Long = {
    var i = -1L

    (info: PartitionHub.ConsumerInfo, each: String) => {
      i += 1
      info.consumerIdByIdx((i % info.size).toInt)
    }
  }

  def partitionToFastestConsumer(): (PartitionHub.ConsumerInfo, String) => Long = {
    (info: PartitionHub.ConsumerInfo, each: String) => info.consumerIds.minBy(id => info.queueSize(id))
  }

  // Attach a PartitionHub Sink to the producer. This will materialize to a corresponding Source
  // We need to use toMat and Keep.right since by default the materialized value to the left is used
  val runnableGraph: RunnableGraph[Source[String, NotUsed]] =
  producer.toMat(PartitionHub.statefulSink(
    //Switch the partitioning function
    //() => partitionRoundRobin(),
    () => partitionToFastestConsumer(),
    startAfterNrOfConsumers = 2, bufferSize = 1))(Keep.right)

  // By running/materializing the producer, we get back a Source, which
  // gives us access to the elements published by the producer
  val fromProducer: Source[String, NotUsed] = runnableGraph.run()

  // Attach three dynamic fan-out sinks to the PartitionHub
  fromProducer.runForeach(msg => println("fast consumer1 received: " + msg))
  fromProducer.throttle(100, 1.millis, 10, ThrottleMode.Shaping)
    .runForeach(msg => println("slow consumer2 received: " + msg))
  fromProducer.throttle(100, 2.millis, 10, ThrottleMode.Shaping)
    .runForeach(msg => println("really slow consumer3 received: " + msg))
}
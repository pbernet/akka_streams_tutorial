package sample.stream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, PartitionHub, RunnableGraph, Source}

import scala.concurrent.duration._

/**
  * Inspired by:
  * http://doc.akka.io/docs/akka/2.5/scala/stream/stream-dynamic.html#using-the-partitionhub
  *
  * Keypoints:
  * - The selection of consumers is done with the roundRobin function
  * - Each element can be routed to only one consumer
  *
  */
object PartitionHubWithDynamicSinks {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  def main(args: Array[String]): Unit = {

    val producer = Source.tick(1.second, 1.second, "message").zipWith(Source(1 to 10))((a, b) => s"$a-$b")

    // New instance of the partitioner function and its state is created for each materialization of the PartitionHub
    def roundRobin(): (PartitionHub.ConsumerInfo, String) => Long = {
      var i = -1L

      (info, elem) => {
        i += 1
        info.consumerIdByIdx((i % info.size).toInt)
      }
    }

    // Attach a PartitionHub Sink to the producer. This will materialize to a corresponding Source
    // We need to use toMat and Keep.right since by default the materialized value to the left is used
    val runnableGraph: RunnableGraph[Source[String, NotUsed]] =
    producer.toMat(PartitionHub.statefulSink(
      () => roundRobin(),
      startAfterNrOfConsumers = 2, bufferSize = 256))(Keep.right)

    // By running/materializing the producer, we get back a Source, which
    // gives us access to the elements published by the producer.
    val fromProducer: Source[String, NotUsed] = runnableGraph.run()

    // Feed three dynamic fan-out sinks into the Hub
    fromProducer.runForeach(msg => println("consumer1: " + msg))
    fromProducer.runForeach(msg => println("consumer2: " + msg))
    fromProducer.runForeach(msg => println("consumer3: " + msg))
  }
}
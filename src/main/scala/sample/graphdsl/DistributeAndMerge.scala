package sample.graphdsl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._

import scala.concurrent.Future
import scala.util.hashing.MurmurHash3


/**
  * Inspired by, stolen from:
  * https://gist.github.com/calvinlfer/cc4ea90328834a95a89ce99aeb998a63
  *
  * Concepts:
  * - Flow that distributes messages (according to a hashing function) across sub-flows
  * - The idea is to have ordered processing per sub-flow but parallel processing across sub-flows
  *
  */
object DistributeAndMerge {
  implicit val system = ActorSystem("DistributeAndMerge")
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  def main(args: Array[String]): Unit = {

    def sampleAsyncCall(x: Int): Future[Int] = Future {
      Thread.sleep((x * 100L) % 10)
      println(s"Async call for value: $x processed by: ${Thread.currentThread().getName}")
      x
    }

    /**
      * Example based on numBuckets = 3
      *                                          --- bucket 1 flow --- ~mapAsync(parallelism)~ ---
      *                   |------------------| /                                                  \|---------------|
      * Open inlet[A] --- | Partition Fan Out|  --- bucket 2 flow --- ~mapAsync(parallelism)~ -----| Merge Fan In  | --- Open outlet[B]
      *                   |------------------| \                                                  /|---------------|
      *                                         --- bucket 3 flow --- ~mapAsync(parallelism)~ ---
      *
      * @param numBuckets  the number of sub-flows to create
      * @param parallelism the mapAsync (ordered) parallelism per sub flow
      * @param hash        the hashing function used to decide
      * @param fn          is the mapping function to be used for mapAsync
      * @tparam A is the input stream of elements of type A
      * @tparam B is the output streams of elements of type B
      * @return a Flow of elements from type A to type B
      */
    def hashingDistribution[A, B](numBuckets: Int,
                                  parallelism: Int,
                                  hash: A => Int,
                                  fn: A => Future[B]): Flow[A, B, NotUsed] = {
      Flow.fromGraph(GraphDSL.create() { implicit builder =>
        import GraphDSL.Implicits._
        val numPorts = numBuckets
        val partitioner =
          builder.add(Partition[A](outputPorts = numPorts, partitioner = a => math.abs(hash(a)) % numPorts))
        val merger = builder.add(Merge[B](inputPorts = numPorts, eagerComplete = false))

        Range(0, numPorts).foreach { eachPort =>
          partitioner.out(eachPort) ~> Flow[A].mapAsync(parallelism)(fn) ~> merger.in(eachPort)
        }

        FlowShape(partitioner.in, merger.out)
      })
    }

    Source(1 to 10)
      .via(
        hashingDistribution[Int, Int](
          numBuckets = 3,
          parallelism = 2,
          hash = element => MurmurHash3.stringHash(element.toString), //Hashing function: String => Int
          fn = sampleAsyncCall
        )
      )
      .runWith(Sink.foreach(each => println(s"Outlet received value: $each")))
      .onComplete(_ => system.terminate())
  }
}

package sample.graphdsl

import java.nio.file.Paths
import java.util.concurrent.ThreadLocalRandom

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.util.{Failure, Success}

/**
  * Show the possibilities of constructing a graph with the GraphDSL
  * http://doc.akka.io/docs/akka/snapshot/scala/stream/stream-graphs.html#constructing-graphs
  *
  * TODO
  * What is the business value of having this?
  * Is there a better example in constructing graphs?
  * sharedDoubler
  */
object WritePrimes extends App {
    implicit val system = ActorSystem("WritePrimes")
    implicit val ec = system.dispatcher

    val maxRandomNumberSize = 100
    val primeSource: Source[Int, akka.NotUsed] =
      Source.fromIterator(() => Iterator.continually(ThreadLocalRandom.current().nextInt(maxRandomNumberSize))).
        filter(rnd => isPrime(rnd)).
        // neighbor +2 is also prime?
        filter(prime => isPrime(prime + 2))

    val fileSink = FileIO.toPath(Paths.get("target/primes.txt"))
    val slowSink = Flow[Int]
      .map(i => { Thread.sleep(100); ByteString(i.toString) })
      .toMat(fileSink)((_, bytesWritten) => bytesWritten)
    val consoleSink = Sink.foreach[Int](println)

    // Optional sample processing flow, to show the nature of the composition
    val sharedDoubler = Flow[Int].map(_ * 2)

    // send primes to both sinks using graph API
    val graph = GraphDSL.create(slowSink, consoleSink)((slow, _) => slow) { implicit builder =>
      (slow, console) =>
        import GraphDSL.Implicits._
        val broadcastSplitter = builder.add(Broadcast[Int](2)) // the splitter - like a Unix tee
        primeSource ~> broadcastSplitter ~> sharedDoubler ~> slow // connect source to splitter, other side to slow sink (via sharedDoubler)
        broadcastSplitter ~> sharedDoubler ~> console // connect other side of splitter to console sink (via sharedDoubler)
        ClosedShape
    }
    val materialized = RunnableGraph.fromGraph(graph).run()

    materialized.onComplete {
      case Success(_) =>
        system.terminate()
      case Failure(e) =>
        println(s"Failure: ${e.getMessage}")
        system.terminate()
    }

  def isPrime(n: Int): Boolean = {
    if (n <= 1) false
    else if (n == 2) true
    else !(2 until n).exists(x => n % x == 0)
  }
}

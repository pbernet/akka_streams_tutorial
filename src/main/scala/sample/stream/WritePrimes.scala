package sample.stream

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString

import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.util.{Failure, Success}

/**
  * Show the possibilites of Constructing Graphs with the GraphDSL
  * http://doc.akka.io/docs/akka/snapshot/scala/stream/stream-graphs.html#constructing-graphs
  *
  */
object WritePrimes {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("Sys")
    import system.dispatcher
    implicit val materializer = ActorMaterializer()

    // generate random numbers
    val maxRandomNumberSize = 1000000
    val primeSource: Source[Int, akka.NotUsed] =
      Source.fromIterator(() => Iterator.continually(ThreadLocalRandom.current().nextInt(maxRandomNumberSize))).
        // filter prime numbers
        filter(rnd => isPrime(rnd)).
        // and neighbor +2 is also prime
        filter(prime => isPrime(prime + 2))

    // write to file sink
    val fileSink = FileIO.toPath(Paths.get("target/primes.txt"))
    val slowSink = Flow[Int]
      // act as if processing is really slow
      .map(i => { Thread.sleep(1000); ByteString(i.toString) })
      .toMat(fileSink)((_, bytesWritten) => bytesWritten)

    // console output sink
    val consoleSink = Sink.foreach[Int](println)

    // Optional sample processing flow, to show the nature of the composition
    val sharedDoubler = Flow[Int].map(_ * 2)

    // send primes to both slow file sink and console sink using graph API
    val graph = GraphDSL.create(slowSink, consoleSink)((slow, _) => slow) { implicit builder =>
      (slow, console) =>
        import GraphDSL.Implicits._
        val broadcast = builder.add(Broadcast[Int](2)) // the splitter - like a Unix tee
        primeSource ~> broadcast ~> sharedDoubler ~> slow // connect primes to splitter, and one side to file (via sample processing flow)
        broadcast ~> sharedDoubler ~> console // connect other side of splitter to console (via sample processing flow)
        ClosedShape
    }
    val materialized = RunnableGraph.fromGraph(graph).run()

    // ensure the output file is closed and the system shutdown upon completion
    materialized.onComplete {
      case Success(_) =>
        system.terminate()
      case Failure(e) =>
        println(s"Failure: ${e.getMessage}")
        system.terminate()
    }
  }

  def isPrime(n: Int): Boolean = {
    if (n <= 1) false
    else if (n == 2) true
    else !(2 until n).exists(x => n % x == 0)
  }
}

package sample.graphdsl

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString

import java.nio.file.Paths
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Construct a graph with the GraphDSL
  * https://doc.akka.io/docs/akka/current/stream/stream-graphs.html?language=scala#constructing-graphs
  *
  */
object WritePrimes extends App {
    implicit val system: ActorSystem = ActorSystem()
    implicit val ec = system.dispatcher

    val maxRandomNumberSize = 100
    val primeSource: Source[Int, akka.NotUsed] =
      Source.fromIterator(() => Iterator.continually(ThreadLocalRandom.current().nextInt(maxRandomNumberSize)))
      .take(100)
      .filter(rnd => isPrime(rnd))
      // neighbor +2 is also prime?
      .filter(prime => isPrime(prime + 2))

  val fileSink = FileIO.toPath(Paths.get("target/primes.txt"))
  val slowSink = Flow[Int]
    .throttle(1, 1.seconds, 1, ThrottleMode.shaping)
    .map(i => ByteString(i.toString))
    .toMat(fileSink)((_, bytesWritten) => bytesWritten)
  val consoleSink = Sink.foreach[Int](each => println(s"Reached console sink: $each"))

  // Additional processing flow, to show the nature of the composition
  val sharedDoubler = Flow[Int].map(_ * 2)

  // send primes to both sinks using graph API
  val graph = GraphDSL.create(slowSink, consoleSink)((x, _) => x) { implicit builder =>
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

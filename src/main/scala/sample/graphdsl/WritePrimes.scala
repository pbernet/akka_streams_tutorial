package sample.graphdsl

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream._
import org.apache.pekko.stream.scaladsl._
import org.apache.pekko.util.ByteString

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
  val primeSource: Source[Int, NotUsed] =
      Source.fromIterator(() => Iterator.continually(ThreadLocalRandom.current().nextInt(maxRandomNumberSize)))
      .take(100)
      .filter(rnd => isPrime(rnd))
      // neighbor +2 is also prime?
      .filter(prime => isPrime(prime + 2))

  val fileSink = FileIO.toPath(Paths.get("target/primes.txt"))
  val slowSink = Flow[Int]
    .throttle(1, 1.seconds, 1, ThrottleMode.shaping)
    .map(i => ByteString(i.toString + "\n"))
    .toMat(fileSink)((_, bytesWritten) => bytesWritten)
  val consoleSink = Sink.foreach[Int](each => println(s"Reached console sink: $each"))

  // Additional processing flow, to show the nature of the composition
  val sharedDoubler = Flow[Int].map(_ * 2)

  // partition primes to both sinks using graph DSL
  // Alternatives:
  // partition:
  // https://doc.akka.io/docs/akka/current/stream/operators/Partition.html
  // alsoTo:
  // https://doc.akka.io/docs/akka/current/stream/stream-flows-and-basics.html
  val graph = GraphDSL.createGraph(slowSink, consoleSink)((_, _)) { implicit builder =>
    (slow, console) =>
      import GraphDSL.Implicits._
      val broadcastSplitter = builder.add(Broadcast[Int](2)) // the splitter - like a Unix tee
      primeSource ~> broadcastSplitter ~> sharedDoubler ~> slow // connect source to splitter, other side to slow sink (via sharedDoubler)
      broadcastSplitter ~> sharedDoubler ~> console // connect other side of splitter to console sink (via sharedDoubler)
      ClosedShape
  }
  val materialized = RunnableGraph.fromGraph(graph).run()

  materialized._2.onComplete {
    case Success(_) =>
      // Grace time to allow writing the last entry to fileSink
      Thread.sleep(500)
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

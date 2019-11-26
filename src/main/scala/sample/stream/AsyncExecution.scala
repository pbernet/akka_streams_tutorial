package sample.stream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}

/**
  * Inspired by:
  * http://akka.io/blog/2016/07/06/threading-and-concurrency-in-akka-streams-explained
  *
  * Each flow stage processes all values (in parallel)
  *
  */
object AsyncExecution {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("AsyncExecution")
    implicit val ec = system.dispatcher

    def stage(name: String): Flow[Int, Int, NotUsed] =
      Flow[Int].map { index =>
        println(s"Stage $name processing $index by ${Thread.currentThread().getName}")
        index
      }


  Source(1 to 10)
    .via(stage("A")).async
    .via(stage("B")).async
    .via(stage("C")).async
    .runWith(Sink.ignore)
    .onComplete(_ => system.terminate())
  }
}
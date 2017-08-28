package sample.stream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}

/**
  * Inspired by:
  * http://www.beyondthelines.net/computing/akka-streams-patterns
  *
  * Each flow stage is executed in parallel
  *
  */
object AsyncExecution {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("Sys")
    implicit val ec = system.dispatcher
    implicit val materializer = ActorMaterializer()

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
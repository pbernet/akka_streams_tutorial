package sample.stream_shared_state

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}

/**
  * Inspired by:
  * https://github.com/akka/akka/issues/19395
  *
  * Deduplicate consecutive elements in a stream using the sliding operator
  *
  * See also: [[Dedupe]] and [[LocalFileCacheCaffeine]]
  */
object DeduplicateConsecutiveElements extends App {
  implicit val system: ActorSystem = ActorSystem()

  val source = Source(List(1, 1, 1, 2, 2, 1, 2, 2, 2, 3, 4, 4, 5, 6))

  val flow = Flow[Int]
    .sliding(2, 1)
    .mapConcat { case prev +: current +: _ =>
      if (prev == current) Nil
      else List(current)
    }

  // prepend this value to the source to avoid losing the first value
  val ignoredValue = Int.MinValue
  val prependedSource = Source.single(ignoredValue).concat(source)
  prependedSource.via(flow).runWith(Sink.foreach(println))
}

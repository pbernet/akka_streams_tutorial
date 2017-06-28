package sample.stream

import java.lang.Exception

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult, ThrottleMode}
import akka.stream.scaladsl.{Sink, Source, SourceQueue, SourceQueueWithComplete}

import scala.concurrent.{Await, Future, TimeoutException}
import scala.concurrent.duration._
import scala.util.Random

/**
  * Source
  * http://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-i
  */

object SendingMessagesToStream {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("Sys")
    implicit val materializer = ActorMaterializer()
    implicit val ec = system.dispatcher


    class SyncQueue[T](q: SourceQueue[T]) {

      /**
        * @throws TimeoutException if it couldn't get the value within `maxWait` time
        */
      def offerBlocking(elem: T, maxWait: Duration = 1.seconds): Future[QueueOfferResult] =
        synchronized {
          //offer returns a Future, which completes with the result of the enqueue operation
          //must only be used from a single thread
          val result = q.offer(elem)
          Await.ready(result, maxWait)
          result
        }
    }

    def asyncOp(userID: Long): Future[String] = {
      Thread.sleep(10) //without waiting time we see a lot of "Future(<not completed>)"
      try {
        if (Random.nextInt() % 2 == 0) {
          println("asyncOp: random exception")
          throw new RuntimeException("random exception")
        }
      } catch {
        case ex: RuntimeException => Future {
          "Exception"
        }
      }
      Future (s"user: $userID")
    }


    val targetQueue =
      Source.queue[Future[String]](Int.MaxValue, OverflowStrategy.backpressure)
        .to(Sink.foreach(println))
        .run()

    val targetSyncQueue = new SyncQueue(targetQueue)

    Source(1 to Int.MaxValue)
      //.throttle(1000, 1.second, 1, ThrottleMode.shaping)
      .mapAsync(1)(x => targetSyncQueue.offerBlocking(asyncOp(x)))
      .runWith(Sink.ignore)
  }
}

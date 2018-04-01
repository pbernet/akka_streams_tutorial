package sample.stream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, DelayOverflowStrategy, OverflowStrategy, QueueOfferResult}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, TimeoutException}


class SyncQueue[T](q: SourceQueue[T]) {

  /**
    * @throws TimeoutException if it couldn't get the value within `maxWait` time
    */
  def offerBlocking(elem: T, maxWait: Duration = 10.seconds): Future[QueueOfferResult] =
    synchronized {
      val result = q.offer(elem)
      Await.ready(result, maxWait)
      result
    }
}

/**
  * Inspired by:
  * http://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-i
  * https://github.com/akka/akka/issues/22397
  * https://github.com/akka/akka/issues/22874
  *
  * Doc on Source.queue
  * https://doc.akka.io/docs/akka/2.5.5/scala/stream/stream-integrations.html#source-queue
  *
  */
object PublishToSourceQueueFromManyThreads {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("PublishToSourceQueueFromManyThreads")
    implicit val ec = system.dispatcher
    implicit val materializer = ActorMaterializer()

    val slowSink: Sink[Seq[String], NotUsed] =
      Flow[Seq[String]]
        .delay(1.seconds, DelayOverflowStrategy.backpressure)
        .to(Sink.foreach(e => println(s"Reached Sink: $e")))

    //The blocking access in SyncQueue, protects the sourceQueue, so even with bufferSize = 0 it "works"
    val sourceQueue = Source.queue[String](bufferSize = 0, OverflowStrategy.backpressure)
      .groupedWithin(10, 2.seconds)
      .to(slowSink)
      .run

    val sourceSyncQueue = new SyncQueue(sourceQueue)

    def publisherThread(index: Int, targetQueue: SyncQueue[String]) = {
      (1 to 10).foreach { each =>
        val offerResult: Future[QueueOfferResult] = targetQueue.offerBlocking(s"$index.$each")
        println(s"Emitted from publisherThread $index.$each with: $offerResult")
      }
    }

    (1 to 100).par.foreach(each => publisherThread(each, sourceSyncQueue))
  }
}

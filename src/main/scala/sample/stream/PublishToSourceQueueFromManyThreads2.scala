package sample.stream

package sample.stream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, DelayOverflowStrategy, OverflowStrategy, QueueOfferResult}

import scala.concurrent.Future
import scala.concurrent.duration._

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
object PublishToSourceQueueFromManyThreads2 {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("PublishToSourceQueueFromManyThreads2")
    implicit val ec = system.dispatcher
    implicit val materializer = ActorMaterializer()

    val slowSink: Sink[Seq[String], NotUsed] =
      Flow[Seq[String]]
        .delay(1.seconds, DelayOverflowStrategy.backpressure)
        .to(Sink.foreach(e => println(s"Reached Sink: $e")))

    //The bufferSize needs to be large enough, otherwise elements get lost, since backpressure does not seem to work as expected
    val sourceQueue: SourceQueueWithComplete[String] = Source.queue[String](bufferSize = 1000, OverflowStrategy.backpressure)
      .groupedWithin(10, 2.seconds)
      .to(slowSink)
      .run

    def publisherThread(index: Int, sourceQueue: SourceQueueWithComplete[String]) = {
      (1 to 10).foreach { each =>
        val offerResult: Future[QueueOfferResult] = sourceQueue.offer(s"$index.$each")
        println(s"Emitted from publisherThread: $index.$each getting offerResult: $offerResult")
      }
    }

    (1 to 100).par.foreach(each => publisherThread(each, sourceQueue))
  }
}

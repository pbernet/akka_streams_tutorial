package sample.stream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source, SourceQueueWithComplete}
import akka.stream.{ActorMaterializer, DelayOverflowStrategy, OverflowStrategy, QueueOfferResult}

import scala.concurrent.duration._

/**
  * Doc:
  * https://doc.akka.io/docs/akka/2.5/stream/stream-integrations.html?language=scala#source-queue
  *
  * Observation:
  *  * The buffer size must be large enough (eg 1000) to allow for massive parallelism in mapAsync (eg 100)
  *  * If buffer size is 1 then the mapAsync can only be run with 1
  *  * With buffer size 1 and mapAsync 100 the queue stops accepting after element 173
  *
  *  Similar example: MergeHubWithDynamicSources
  */

object PublishToSourceQueueFromStream {
  implicit val system = ActorSystem("PublishToSourceQueueFromStream")
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  def main(args: Array[String]): Unit = {

    val bufferSize = 1000

    val slowSink: Sink[Seq[Int], NotUsed] =
      Flow[Seq[Int]]
        .delay(1.seconds, DelayOverflowStrategy.backpressure)
        .to(Sink.foreach(e => println(s"Reached Sink: $e")))

    val queue: SourceQueueWithComplete[Int] = Source
      .queue[Int](bufferSize, OverflowStrategy.backpressure)
      .groupedWithin(10, 2.seconds)
      .to(slowSink)
      .run

    Source(1 to 1000)
      .mapAsync(100)(x ⇒ {
      queue.offer(x).map {
        case QueueOfferResult.Enqueued    ⇒ println(s"enqueued $x")
        case QueueOfferResult.Dropped     ⇒ println(s"dropped $x")
        case QueueOfferResult.Failure(ex) ⇒ println(s"Offer failed ${ex.getMessage}")
        case QueueOfferResult.QueueClosed ⇒ println("Source Queue closed")
      }
    }).runWith(Sink.ignore)
  }
}
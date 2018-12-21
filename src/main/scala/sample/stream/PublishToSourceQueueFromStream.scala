package sample.stream

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source, SourceQueueWithComplete}
import akka.stream.{ActorMaterializer, DelayOverflowStrategy, OverflowStrategy, QueueOfferResult}
import akka.{Done, NotUsed}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Doc:
  * https://doc.akka.io/docs/akka/2.5/stream/stream-integrations.html?language=scala#source-queue
  *
  * Observations:
  *  * The bufferSize should be large enough
  *  * With bufferSize = 1, the parallelism can not be > 1 - queue stops accepting elements with failures (on my machine after 173)
  *
  *  Similar example: MergeHubWithDynamicSources
  */

object PublishToSourceQueueFromStream {
  implicit val system = ActorSystem("PublishToSourceQueueFromStream")
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  def main(args: Array[String]): Unit = {

    val bufferSize = 100
    val parallelism = 1

    val slowSink: Sink[Seq[Int], NotUsed] =
      Flow[Seq[Int]]
        .delay(1.seconds, DelayOverflowStrategy.backpressure)
        .to(Sink.foreach(e => println(s"Reached Sink: $e")))

    val queue: SourceQueueWithComplete[Int] = Source
      .queue[Int](bufferSize, OverflowStrategy.backpressure)
      .groupedWithin(10, 2.seconds)
      .to(slowSink)
      .run


    val doneConsuming: Future[Done] = queue.watchCompletion()
    signalWhen(doneConsuming, "consuming") //never completes...

    val donePublishing: Future[Done] = Source(1 to 1000)
      .mapAsync(parallelism){each => {
      queue.offer(each).map {
        case QueueOfferResult.Enqueued    => println(s"enqueued $each")
        case QueueOfferResult.Dropped     => println(s"dropped $each")
        case QueueOfferResult.Failure(ex) => println(s"Offer failed ${ex.getMessage}")
        case QueueOfferResult.QueueClosed => println("Source Queue closed")
      }
    }}.runWith(Sink.ignore)
    signalWhen(donePublishing, "publishing")
  }

  def signalWhen(done: Future[Done], operation: String) = {
    done.onComplete {
      case Success(b) =>
        println(s"Finished: $operation")
      case Failure(e) =>
        println(s"Failure: ${e.getMessage}. About to terminate...")
        system.terminate()
    }
  }
}
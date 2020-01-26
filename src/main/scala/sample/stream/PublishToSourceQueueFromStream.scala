package sample.stream

import akka.actor.ActorSystem
import akka.stream.Supervision.Decider
import akka.stream._
import akka.stream.scaladsl.{Flow, Sink, Source, SourceQueueWithComplete}
import akka.{Done, NotUsed}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * n parallel publishing clients -> sourceQueue -> slowSink
  *
  * Doc:
  * https://doc.akka.io/docs/akka/current/stream/actor-interop.html?language=scala#source-queue
  *
  * Doc buffers:
  * https://doc.akka.io/docs/akka/current/stream/stream-rate.html#buffers-in-akka-streams
  *
  */

object PublishToSourceQueueFromStream extends App {
  implicit val system = ActorSystem("PublishToSourceQueueFromStream")
  implicit val ec = system.dispatcher

  val bufferSize = 100
  val parallelism = 10

  val slowSink: Sink[Seq[Int], NotUsed] =
    Flow[Seq[Int]]
      .delay(1.seconds, DelayOverflowStrategy.backpressure)
      .to(Sink.foreach(e => println(s"Reached sink: $e")))

  val sourceQueue: SourceQueueWithComplete[Int] = Source
    .queue[Int](bufferSize, OverflowStrategy.backpressure)
    .groupedWithin(10, 2.seconds)
    .to(slowSink)
    .run

  val doneConsuming: Future[Done] = sourceQueue.watchCompletion()
  signalWhen(doneConsuming, "consuming") //never completes...


  simulatePublishingClientsFromStream()

  //Does not finish, because is not able to handle backpressure signals
  //simulatePublishingClientsSimple()



  //We need to decide on the stream level, because the OverflowStrategy.backpressure
  //on the sourceQueue causes an IllegalStateException
  //Handling this on the stream level allows to restart the stream
  private def simulatePublishingClientsFromStream() = {

    val decider: Decider = {
      case _: IllegalStateException => println("Got backpressure signal for offered element, restart..."); Supervision.Restart
      case _ => Supervision.Stop
    }

    val donePublishing: Future[Done] = Source(1 to 1000)
      .mapAsync(parallelism)(offerToSourceQueue)
      .withAttributes(ActorAttributes.supervisionStrategy(decider))
      .runWith(Sink.ignore)
    signalWhen(donePublishing, "publishing")
  }

  private def simulatePublishingClientsSimple() = (1 to 1000).par.foreach(offerToSourceQueue)

  private def offerToSourceQueue(each: Int) = {
    sourceQueue.offer(each).map {
      case QueueOfferResult.Enqueued => println(s"enqueued $each")
      case QueueOfferResult.Dropped => println(s"dropped $each")
      case QueueOfferResult.Failure(ex) => println(s"Offer failed: $ex")
      case QueueOfferResult.QueueClosed => println("Source Queue closed")
    }
  }

  private def signalWhen(done: Future[Done], operation: String) = {
    done.onComplete {
      case Success(b) =>
        println(s"Finished: $operation")
      case Failure(e) =>
        println(s"Failure: $e About to terminate...")
        system.terminate()
    }
  }
}
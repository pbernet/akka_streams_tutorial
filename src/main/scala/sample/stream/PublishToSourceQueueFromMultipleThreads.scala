package sample.stream

import akka.actor.ActorSystem
import akka.stream.Supervision.Decider
import akka.stream._
import akka.stream.scaladsl.{Flow, Sink, Source, SourceQueueWithComplete}
import akka.{Done, NotUsed}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.parallel.CollectionConverters._
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
  * Open issue:
  * https://github.com/akka/akka/issues/26696
  */
object PublishToSourceQueueFromMultipleThreads extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("PublishToSourceQueueFromMultipleThreads")
  implicit val ec = system.dispatcher

  val bufferSize = 100
  // As of akka 2.6.x there is a thread safe implementation for SourceQueue
  val maxConcurrentOffers = 1000
  val numberOfPublishingClients = 1000

  val slowSink: Sink[Seq[Int], NotUsed] =
    Flow[Seq[Int]]
      .delay(2.seconds, DelayOverflowStrategy.backpressure)
      .to(Sink.foreach(e => logger.info(s"Reached sink: $e")))

  val sourceQueue: SourceQueueWithComplete[Int] = Source
    .queue[Int](bufferSize, OverflowStrategy.backpressure, maxConcurrentOffers)
    .groupedWithin(10, 1.seconds)
    .to(slowSink)
    .run()

  val doneConsuming: Future[Done] = sourceQueue.watchCompletion()
  signalWhen(doneConsuming, "consuming") //never completes

  simulatePublishingFromMulitpleThreads()

  // Before 2.6.x a stream had to be used to throttle and control the backpressure
  //simulatePublishingClientsFromStream()

  // Decide on the stream level, because the OverflowStrategy.backpressure
  // on the sourceQueue causes an IllegalStateException
  // Handling this on the stream level allows to restart the stream
  private def simulatePublishingClientsFromStream() = {

    val decider: Decider = {
      case _: IllegalStateException => println("Got backpressure signal for offered element, restart..."); Supervision.Restart
      case _ => Supervision.Stop
    }

    val donePublishing: Future[Done] = Source(1 to numberOfPublishingClients)
      .mapAsync(10)(offerToSourceQueue) //throttle
      .withAttributes(ActorAttributes.supervisionStrategy(decider))
      .runWith(Sink.ignore)
    signalWhen(donePublishing, "publishing")
  }

  private def simulatePublishingFromMulitpleThreads() = (1 to numberOfPublishingClients).par.foreach(offerToSourceQueue)

  private def offerToSourceQueue(each: Int) = {
    sourceQueue.offer(each).map {
      case QueueOfferResult.Enqueued => logger.info(s"enqueued $each")
      case QueueOfferResult.Dropped => logger.info(s"dropped $each")
      case QueueOfferResult.Failure(ex) => logger.info(s"Offer failed: $ex")
      case QueueOfferResult.QueueClosed => logger.info("Source Queue closed")
    }
  }

  private def signalWhen(done: Future[Done], operation: String) = {
    done.onComplete {
      case Success(b) =>
        logger.info(s"Finished: $operation")
      case Failure(e) =>
        logger.info(s"Failure: $e About to terminate...")
        system.terminate()
    }
  }
}
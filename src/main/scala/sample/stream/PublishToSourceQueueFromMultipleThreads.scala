package sample.stream

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.{Flow, Keep, MergeHub, Sink, Source}
import org.apache.pekko.{Done, NotUsed}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.parallel.CollectionConverters.*
import scala.concurrent.Future
import scala.concurrent.duration.*
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
  * Similar example: [[MergeHubWithDynamicSources]]
  *
  * Open issue:
  * https://github.com/akka/akka/issues/26696
  *
  * See also:
  * [[BoundedSourceQueue]] (= a sync variant of SourceQueue with OverflowStrategy.dropNew)
  *
  */
object PublishToSourceQueueFromMultipleThreads {
  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    implicit val system: ActorSystem = ActorSystem()

    import system.dispatcher

    val bufferSize = 100
    val numberOfPublishingClients = 1000

    val slowSink: Sink[Seq[Int], NotUsed] =
      Flow[Seq[Int]]
        .delay(2.seconds, DelayOverflowStrategy.backpressure)
        .to(Sink.foreach(e => logger.info(s"Reached sink: $e")))

    val (producerSink: Sink[Int, NotUsed], doneConsuming: Future[Done]) =
      MergeHub
        .source[Int](perProducerBufferSize = bufferSize)
        .groupedWithin(10, 1.seconds)
        .watchTermination(Keep.both)
        .to(slowSink)
        .run()

    signalWhen(doneConsuming, "consuming")

    simulatePublishingFromMultipleThreads()

    def simulatePublishingFromMultipleThreads(): Unit = {
      (1 to numberOfPublishingClients).par.foreach(offerToSourceQueue)
    }

    def offerToSourceQueue(each: Int): Unit = {
      Source.single(each)
        .watchTermination(Keep.right)
        .toMat(producerSink)(Keep.left)
        .run()
        .onComplete {
          case Success(_) => logger.info(s"enqueued $each")
          case Failure(exception) => logger.info(s"Offer failed: $exception")
        }
    }

    def signalWhen(done: Future[Done], operation: String): Unit = {
      done.onComplete {
        case Success(_) =>
          logger.info(s"Finished: $operation")
        case Failure(e) =>
          logger.info(s"Failure: $e About to terminate...")
          system.terminate()
      }
    }
  }
}

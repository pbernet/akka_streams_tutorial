package sample.stream

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.Paths
import java.time.LocalTime
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Run a fast and two slow flows with the same elements in parallel
  * and wait for the flows to complete using for comprehension or Futures
  *
  * Remarks:
  *  - Use custom dispatcher for slow FileIO flows
  *    See [[actor.BlockingRight]] for use of custom dispatcher in typed Actor
  *  - Shows finalisation strategies when using Futures:
  *    Stop when all flows are OK, or one is NOK
  *    vs
  *    Stop when all flows are OK, or NOK
  *
  */
object WaitForFlowsToComplete extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  def lineSink(filename: String): Sink[String, Future[IOResult]] =
    Flow[String]
      .map(s => ByteString(s + "\n"))
      .wireTap(_ => logger.info(s"Add line to file: $filename"))
      .toMat(FileIO.toPath(Paths.get(filename)))(Keep.right) //retain to the Future[IOResult]
      .withAttributes(ActorAttributes.dispatcher("custom-dispatcher-for-blocking"))

  val origSource = Source(1 to 10)

  // scan (= transform) the source
  val factorialsSource = origSource.scan(BigInt(1))((acc, next) => acc * next)

  val fastFlow = origSource.runForeach(i => logger.info(s"Reached sink: $i"))

  val slowFlow = factorialsSource
    .map(_.toString)
    .throttle(1, 1.second, 1, ThrottleMode.shaping)
    .mapAsync(parallelism = 1)(each => simulateFaultyProcessing(each))
    .runWith(lineSink("factorial1.txt"))

  val slowFaultyFlow = factorialsSource
    .map(_.toString)
    .throttle(1, 1.second, 1, ThrottleMode.shaping)
    .runWith(lineSink("factorial2.txt"))

  //processWithForComprehension()

  processWithFutureSequence()

  private def processWithForComprehension() = {
    val allDone = for {
      fastFlowDone <- fastFlow
      slowFlow1Done <- slowFlow
      slowFlow2Done <- slowFaultyFlow
    } yield (fastFlowDone, slowFlow1Done, slowFlow2Done)

    allDone.onComplete { results =>
      logger.info(s"Resulting futures from flows: $results - about to terminate")
      system.terminate()
    }
  }

  private def processWithFutureSequence() = {

    // completes when either:
    // - all the futures have completed successfully, or
    // - one of the futures has failed
    val futSeq = Future.sequence(List(fastFlow, slowFlow, slowFaultyFlow)
      // Lifting each Flow result to a Try allows to wait for all results (OK or NOK)
      // see: https://stackoverflow.com/questions/29344430/scala-waiting-for-sequence-of-futures
      //.map(each => each.transform(Success(_)))
    )

    futSeq.onComplete {
      case Success(b) =>
        logger.info(s"Success: $b")
        system.terminate()
      case Failure(e) =>
        logger.info(s"Failure. Exception message: ${e.getMessage}")
        system.terminate()
    }
  }

  private def simulateFaultyProcessing(each: String) = {
    val time = LocalTime.now()
    logger.info(s"Processing at: $time")
    if (time.getSecond % 2 == 0) {
      logger.info(s"RuntimeException at: $time")
      throw new RuntimeException("BOOM - RuntimeException")
    }
    Future(each)
  }
}

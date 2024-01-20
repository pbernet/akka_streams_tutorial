package sample.stream

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream._
import org.apache.pekko.stream.scaladsl._
import org.apache.pekko.util.ByteString
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.Paths
import java.time.LocalTime
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Run a fast and a slow flow as well as a slow faulty flow
  * with the same elements in parallel
  * Wait for the flows to complete using for comprehension or Futures
  * Processing with Futures allows for more control
  *
  * Remarks:
  *  - Use custom dispatcher for slow FileIO flows
  *    See [[actor.BlockingRight]] for use of custom dispatcher in typed Actor
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

  val fastFlow = origSource.runForeach(i => logger.info(s"Reached fast sink: $i"))

  val slowFlow = factorialsSource
    .map(_.toString)
    .throttle(1, 1.second, 1, ThrottleMode.shaping)
    .runWith(lineSink("factorials_slow.txt"))

  val slowFaultyFlow = factorialsSource
    .map(_.toString)
    .throttle(1, 1.second, 1, ThrottleMode.shaping)
    .map(each => simulateFaultyProcessing(false, each))
    .runWith(lineSink("factorial_slow_faulty.txt"))

  //processWithForComprehension()

  processWithFutureSequence()

  private def processWithForComprehension() = {
    val allDone = for {
      slowFaultyFlowDone <- slowFaultyFlow
      slowFlowDone <- slowFlow
      fastFlowDone <- fastFlow
    } yield (fastFlowDone, slowFlowDone, slowFaultyFlowDone)

    allDone.onComplete {
      case Success(r) =>
        logger.info(s"Success. Flow results: $r")
        system.terminate()
      case Failure(e) =>
        logger.info(s"Failure. Exception message: ${e.getMessage}")
      system.terminate()
    }
  }

  // Allows for more control
  private def processWithFutureSequence() = {

    // completes when either:
    //  - all futures have completed successfully, or
    //  - one of the futures has failed
    val futSeq = Future.sequence(List(fastFlow, slowFlow, slowFaultyFlow)
      // Lifting each flow result to a Try allows to wait for *all* flow results (Success OR Failure)
      // see: https://stackoverflow.com/questions/29344430/scala-waiting-for-sequence-of-futures
      //.map(each => each.transform(Success(_)))
    )

    futSeq.onComplete {
      case Success(r) =>
        logger.info(s"Success. Flow results: $r")
        system.terminate()
      case Failure(e) =>
        logger.info(s"Failure. Exception message: ${e.getMessage}")
        system.terminate()
    }
  }

  private def simulateFaultyProcessing(isFaulty: Boolean, payload: String) = {
    if (isFaulty) {
    val time = LocalTime.now()
      logger.info(s"Processing $payload at: $time")
    if (time.getSecond % 2 == 0) {
      logger.info(s"RuntimeException at: $time")
      throw new RuntimeException("BOOM - RuntimeException")
    }
      payload
    } else payload
  }
}

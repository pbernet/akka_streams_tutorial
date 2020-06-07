package sample.stream

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent._
import scala.concurrent.duration._

/**
  *  Run a fast and two slow flows with the same data and wait for all of them to complete.
  *  Use custom dispatcher for slow FileIO flows.
  *
  *  See [[actor.BlockingRight]] for use of custom dispatcher in typed Actor
  */
object WaitForThreeFlowsToComplete extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("WaitForThreeFlowsToComplete")
  implicit val ec = system.dispatcher

  def lineSink(filename: String): Sink[String, Future[IOResult]] =
    Flow[String]
      .map(s => ByteString(s + "\n"))
      .wireTap(_ => logger.info(s"Add line to file: $filename"))
      .toMat(FileIO.toPath(Paths.get(filename)))(Keep.right) //retain to the Future[IOResult]
      .withAttributes(ActorAttributes.dispatcher("custom-dispatcher-for-blocking"))

  val origSource = Source(1 to 10)
  //scan (= transform) the source
  val factorialsSource = origSource.scan(BigInt(1))((acc, next) => acc * next)

  val fastFlow = origSource.runForeach(i => logger.info(s"Reached sink: $i"))

  val slowFlow1 = factorialsSource
    .map(_.toString)
    .runWith(lineSink("factorial1.txt"))

  val slowFlow2 = factorialsSource
    .zipWith(Source(0 to 10))((num, idx) => s"$idx! = $num")
    .throttle(1, 1.second, 1, ThrottleMode.shaping)
    .runWith(lineSink("factorial2.txt"))

  val allDone = for {
    fastFlowDone <- fastFlow
    slowFlow1Done <- slowFlow1
    slowFlow2Done <- slowFlow2
  } yield (fastFlowDone, slowFlow1Done, slowFlow2Done)

  allDone.onComplete { results =>
    logger.info(s"Resulting futures from flows: $results - about to terminate")
    system.terminate()
  }
}

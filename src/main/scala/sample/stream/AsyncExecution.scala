package sample.stream

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorAttributes
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Each stream element is processed by each flow stage A/B/C (in parallel)
  * Show the effects of using a custom dispatcher on stage B to guard (potentially) blocking behaviour
  *
  * Inspired by:
  * http://akka.io/blog/2016/07/06/threading-and-concurrency-in-akka-streams-explained
  *
  * See also [[sample.stream.WaitForFlowsToComplete]]
  *
  */
object AsyncExecution extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  def stage(name: String) =
    Flow[Int]
      .wireTap(index => logger.info(s"Stage $name processing element $index by ${Thread.currentThread().getName}"))

  def stageBlocking(name: String) =
    Flow[Int]
      .wireTap(index => logger.info(s"Stage $name processing element $index by ${Thread.currentThread().getName}"))
      .wireTap(_ => Thread.sleep(5000))
      .withAttributes(ActorAttributes.dispatcher("custom-dispatcher-for-blocking"))

  def sinkBlocking: Sink[Int, Future[Done]] =
    Sink.foreach { index: Int =>
      Thread.sleep(2000)
      logger.info(s"Slow sink processing element $index by ${Thread.currentThread().getName}")
    }
      //Adding a custom dispatcher creates an async boundary
      //see discussion in: https://discuss.lightbend.com/t/how-can-i-make-sure-that-fileio-frompath-is-picking-up-my-dispatcher/6528/4
      .withAttributes(ActorAttributes.dispatcher("custom-dispatcher-for-blocking"))


  val done = Source(1 to 10)
    .via(stage("A")).async
    //When activated instead of alsoTo(sinkBlocking): elements for stage C are held up by stage B
    //.via(stageBlocking("B")).async
    .alsoTo(sinkBlocking).async
    .via(stage("C")).async
    .runWith(Sink.ignore)

  //With alsoTo(sinkBlocking) the stages A and C signal "done" too early and thus would terminate the whole stream
  //The reason for this is the custom dispatcher in sinkBlocking
  //terminateWhen(done)

  def terminateWhen(done: Future[_]) = {
    done.onComplete {
      case Success(_) =>
        println("Flow Success. About to terminate...")
        system.terminate()
      case Failure(e) =>
        println(s"Flow Failure: $e. About to terminate...")
        system.terminate()
    }
  }
}
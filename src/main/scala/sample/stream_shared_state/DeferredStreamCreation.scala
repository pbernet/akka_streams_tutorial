package sample.stream_shared_state

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Doc:
  * https://doc.akka.io/docs/akka/current/stream/operators/Flow/futureFlow.html?
  * https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/prefixAndTail.html
  *
  * Deferred stream creation of tail elements based on the first element
  *
  * Similar to: [[HandleFirstElementSpecially]]
  */
object DeferredStreamCreation extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val source = Source(List(1, 2, 3, 4, 5))
  val printSink = Sink.foreach[String](each => println(s"Reached sink: $each"))

  def processingFlow(id: Int): Future[Flow[Int, String, NotUsed]] = {
    println("About to process tail elements...")
    Thread.sleep(2000)
    Future(Flow[Int].map(n => s"head element: $id, tail element: $n"))
  }

  val doneDelayed =
    source.prefixAndTail(1).flatMapConcat {
      case (Seq(id), tailSource) =>
        // process all tail elements once the first element is here
        tailSource.via(Flow.futureFlow(processingFlow(id)))
    }.runWith(printSink)

  terminateWhen(doneDelayed)

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

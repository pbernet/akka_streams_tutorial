package sample.stream_shared_state

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future
import scala.util.{Failure, Success}


/**
  * Inspired by:
  * https://stackoverflow.com/questions/40743047/handle-akka-streams-first-element-specially
  * Doc:
  * https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/prefixAndTail.html
  *
  * Similar to: [[DeferredStreamCreation]]
  *
  */
object HandleFirstElementSpecially extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val source = Source(List(1, 2, 3, 4, 5))
  val first = Flow[Int].map(i => s"Processed first: $i")
  val rest = Flow[Int].map(i => s"Processed rest: $i")

  val printSink = Sink.foreach[String](each => println(s"Reached sink: $each"))

  val done = source.prefixAndTail(1).flatMapConcat { case (head, tail) =>
    // `head` is a Seq of prefix element(s), processed via `first` flow
    // `tail` is a Seq of tail elements, processed via `rest` flow
    // process head and tail elements with separate flows and concat results
    Source(head).via(first).concat(tail.via(rest))
  }.runWith(printSink)

  terminateWhen(done)

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

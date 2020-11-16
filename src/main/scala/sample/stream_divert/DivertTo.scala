package sample.stream_divert

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Inspired by:
  * Colin Breck talk scala days NY 2018
  *
  * Concepts:
  *  - treat errors as data by using Either
  *  - divert invalid elements at the end (instead of filtering/dropping earlier)
  *  - keep order of elements downstream
  */
object DivertTo extends App {
  implicit val system = ActorSystem("DivertTo")
  implicit val executionContext = system.dispatcher

  val source = Source(1 to 10)

  val sink = Sink.foreach[Either[Valid[Int], Invalid[Int]]](each => println(s"Reached sink: ${each.swap.getOrElse(0)}"))

  val errorSink = Flow[Invalid[Int]]
    .map(each => println(s"Reached errorSink: $each"))
    .to(Sink.ignore)

  val flow: Flow[Int, Either[Valid[Int], Invalid[Int]], NotUsed] = Flow[Int]
    .map { x =>
      if (x % 2 == 0) Left(Valid(x))
      else Right(Invalid(x, Some(new Exception("Is odd"))))
    }
    .map {
      //Drawback of this approach: Pattern matching on all downstream operations
      case left@Left(_) => businessLogicOn(left)
      case right@Right(_) => right
    }
    .map {
      case left@Left(_) => left
      case right@Right(_) => right
    }
    //Divert invalid elements
    //contramap: apply "getOrElse" to each incoming upstream element *before* it is passed to the errorSink
    .divertTo(errorSink.contramap(_.getOrElse(Invalid(0, Some(new Exception("N/A"))))), _.isRight)

  private def businessLogicOn(left: Left[Valid[Int], Invalid[Int]]) = {
    if (left.value.payload > 5) left
    else Right(Invalid(left.value.payload, Some(new Exception("Is smaller than 5"))))
  }

  val done = source.via(flow).runWith(sink)
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

case class Valid[T](payload: T)

case class Invalid[T](payload: T, cause: Option[Throwable])

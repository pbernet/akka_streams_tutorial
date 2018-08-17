package sample.stream_divert

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}


case class Valid[T](payload: T)
case class Invalid[T](payload: T, cause: Option[Throwable])

/**
  * Inspired by:
  * Colin Breck talk scala days NY 2018
  *
  * Concepts:
  *  * divert invalid elements instead of filtering/dropping them
  *  * keep order of elements downstream
  */
object DivertToExample {
  implicit val system = ActorSystem("DivertToExample")
  import system.dispatcher
  implicit val materializer = ActorMaterializer()

  def main(args: Array[String]): Unit = {

    val source = Source(1 to 10)

    val sink = Sink.foreach[Either[Valid[Int], Invalid[Int]]](each => println(s"${each.left.get} is valid"))

    val errorSink =  Flow[Invalid[Int]]
      .map(each => println(s"$each is invalid, because of: ${each.cause.getOrElse(new Throwable("N/A"))}"))
      .to(Sink.ignore)

    val flow: Flow[Int, Either[Valid[Int], Invalid[Int]], NotUsed] = Flow[Int]
      .map {  x =>
      if (x % 2 == 0) Left(Valid(x))
      else Right(Invalid(x, Some(new Throwable("Is odd"))))
      }
      .map {
        //Drawback of this approach: Pattern matching on downstream operations
        case left@Left(_) => left
        case right@Right(_) => right
      }
      //contramap: apply "isRight" to each incoming upstream element before it is passed to the sink
      .divertTo(errorSink.contramap(_.right.get), _.isRight)

    val result = source.via(flow).runWith(sink)
    result.onComplete(_ => system.terminate())
  }
}

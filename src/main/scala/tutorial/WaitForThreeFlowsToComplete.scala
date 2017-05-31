package tutorial

import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import akka.{Done, NotUsed}

import scala.concurrent.duration._

import scala.concurrent._

object WaitForThreeFlowsToComplete {

  def main(args: Array[String]) = {

    implicit val system = ActorSystem("WaitForThreeFlowsToComplete")
    implicit val ec = system.dispatcher
    implicit val materializer = ActorMaterializer()

    def reusableLineSink(filename: String): Sink[String, Future[IOResult]] =
      Flow[String]
        .map(s => ByteString(s + "\n"))
        //Keep.right means: we want to retain what the FileIO.toPath sink has to offer
        .toMat(FileIO.toPath(Paths.get(filename)))(Keep.right)


    val source: Source[Int, NotUsed] = Source(1 to 100)

    val f1Fut: Future[Done] = source.runForeach(i => println(i))

    //declaration of what happens when we scan (= transform) the source
    val factorials = source.scan(BigInt(1))((acc, next) => acc * next)
    val f2fut: Future[IOResult] = factorials.map(_.toString).runWith(reusableLineSink("factorial2.txt"))

    val f3fut = factorials
      .zipWith(Source(0 to 10))((num, idx) => s"$idx! = $num")
      .throttle(1, 1.second, 1, ThrottleMode.shaping)
      .runWith(reusableLineSink("factorial3.txt"))

    val aggFut = for {
      f1Result <- f1Fut
      f2Result <- f2fut
      f3Result <- f3fut
    } yield (f1Result, f2Result, f3Result)

    aggFut.onComplete{  results =>
      println("Resulting Futures from Flows completed with results: " + results + " - about to terminate")
      system.terminate()
    }}
}

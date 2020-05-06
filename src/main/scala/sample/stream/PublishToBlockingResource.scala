package sample.stream

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.DelayOverflowStrategy
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.duration._
import scala.util.Failure


/**
  * n parallel clients -> blocking resource (simulated by java.util.concurrent.BlockingQueue) -> slowSink
  *
  * Doc:
  * https://doc.akka.io/docs/akka/current/stream/operators/Source/unfoldResource.html
  */
object PublishToBlockingResource extends App {
  implicit val system = ActorSystem("PublishToBlockingResource")
  implicit val ec = system.dispatcher

  val slowSink: Sink[Seq[Int], NotUsed] =
    Flow[Seq[Int]]
      .delay(1.seconds, DelayOverflowStrategy.backpressure)
      .to(Sink.foreach(e => println(s"Reached sink: $e")))

  val blockingResource: BlockingQueue[Int] = new ArrayBlockingQueue[Int](100)

  //Start a new `Source` from some (third party) blocking resource which can be opened, read and closed
  val source: Source[Int, NotUsed] =
    Source.unfoldResource[Int, BlockingQueue[Int]](
      () => blockingResource,                   //open
      (q: BlockingQueue[Int]) => Some(q.take()),//read
      (_: BlockingQueue[Int]) => {})            //close

  val done = source
    .groupedWithin(10, 2.seconds)
    .watchTermination()((_, done) => done.onComplete {
      case Failure(err) =>
        println(s"Flow failed: $err")
      case each => println(s"Server flow terminated: $each")
    })
    .runWith(slowSink)

  //simulate n process that publish in blocking fashion to the queue
  (1 to 1000).par.foreach(value => blockingResource.put(value))
}
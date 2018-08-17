package sample.stream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, DelayOverflowStrategy, OverflowStrategy}

import scala.concurrent.duration._

/**
  * Doc:
  * https://doc.akka.io/docs/akka/2.5/stream/stream-integrations.html?language=scala#source-queue
  *
  */

object PublishToSourceQueueFromStream {
  implicit val system = ActorSystem("PublishToSourceQueueFromStream")
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  def main(args: Array[String]): Unit = {

    val slowSink: Sink[Seq[String], NotUsed] =
      Flow[Seq[String]]
        .delay(1.seconds, DelayOverflowStrategy.backpressure)
        .to(Sink.foreach(e => println(s"Reached Sink: $e")))

    val sourceQueue = Source.queue[String](bufferSize = 5, OverflowStrategy.backpressure)
      .groupedWithin(10, 2.seconds)
      .to(slowSink)
      .run

     Source(1 to 100)
       //simulates many threads
      .mapAsync(5)(x => sourceQueue.offer(x.toString))
      .runWith(Sink.ignore)

    //Should work as well
    //(1 to 100).par.foreach(each => sourceQueue.offer(each.toString))
  }
}

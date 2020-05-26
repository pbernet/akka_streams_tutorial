package sample.stream_shared_state

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source, SourceQueueWithComplete, Zip}
import akka.stream.{FlowShape, OverflowStrategy}

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._


/**
  * Source -> Flow(parameter) -> Sink
  *
  * Inject parameter from outside the flow execution.
  * Interesting also the use of the extrapolate operator.
  *
  * Similar to [[Blacklist]]
  *
  * Inspired by:
  * https://discuss.lightbend.com/t/how-to-configure-flow-on-the-fly/6554/2
  *
  */
object ParametrizedFlow extends App {
  implicit val as = ActorSystem("ParametrizedFlow")

  def createUserParamedFlow[A, P, O](bufferSize: Int, overflowStrategy: OverflowStrategy, initialParam: P)(fun: (A, P) => O) =
    Flow.fromGraph(GraphDSL.create(Source.queue[P](bufferSize, overflowStrategy)) { implicit builder =>
      queue =>
        import GraphDSL.Implicits._
        val zip = builder.add(Zip[A, P]())
        //based on https://doc.akka.io/docs/akka/current/stream/stream-rate.html#understanding-extrapolate-and-expand
        val extra = builder.add(Flow[P].extrapolate(Iterator.continually(_), Some(initialParam)))
        val map = builder.add(Flow[(A, P)].map(r => fun(r._1, r._2)))

        queue ~> extra ~> zip.in1
        zip.out ~> map
        FlowShape(zip.in0, map.out)
    })


  val fun = (a: Int, b: Double) => a * b
  val flow: ((Cancellable, SourceQueueWithComplete[Double]), Future[immutable.Seq[Double]]) =
    Source.tick(0.seconds, 500.millis, 10)
      .viaMat(createUserParamedFlow(1, OverflowStrategy.dropBuffer, 0.5)(fun))(Keep.both)
      .wireTap(x => println(x))
      .toMat(Sink.seq)(Keep.both)
      .run()

  Thread.sleep(5000)
  flow._1._2.offer(1.0) //this is how you set params
  Thread.sleep(2000)
  flow._1._2.offer(1.5)
  Thread.sleep(2000)
  flow._1._1.cancel() //stop the "test"
  Thread.sleep(1000)
  println(flow._2) //whole output
}

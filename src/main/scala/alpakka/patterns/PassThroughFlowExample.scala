package alpakka.patterns

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Source, _}
import akka.stream.{FlowShape, Graph, OverflowStrategy}

import java.time.LocalDateTime

/**
  * Doc:
  * https://doc.akka.io/docs/alpakka/current/patterns.html#passthrough
  *
  * Use [[PassThroughFlow]] to pass original value to subsequent stage
  *
  * Applied in [[alpakka.kafka.WordCountConsumer]]
  *
  */
object PassThroughFlowExample extends App {
  implicit val system: ActorSystem = ActorSystem()

  val sourceOfOriginalValues = Source(1 to 100)
    .map(origValue => (origValue.toString, LocalDateTime.now()))

  val esotericSlowFlow = Flow[(String, LocalDateTime)]
    .buffer(1, OverflowStrategy.dropHead)
    .map { s => Thread.sleep(2000); s }
    .scan(Map[String, LocalDateTime]())((m, s) => m + (s._1 -> s._2))
    .extrapolate(Iterator.continually(_), Some(Map.empty)) // no backpressure, emit always a element
    .buffer(1, OverflowStrategy.dropHead)

  sourceOfOriginalValues.via(PassThroughFlow(esotericSlowFlow))
    .runWith(Sink.foreach(t => println(s"Reached sink: originalValue: ${t._2}, resultMap: ${t._1}")))
}

object PassThroughFlow {
  def apply[A, T](processingFlow: Flow[A, T, NotUsed]): Graph[FlowShape[A, (T, A)], NotUsed] =
    apply[A, T, (T, A)](processingFlow, Keep.both)

  def apply[A, T, O](processingFlow: Flow[A, T, NotUsed], output: (T, A) => O): Graph[FlowShape[A, O], NotUsed] =
    Flow.fromGraph(GraphDSL.create() { implicit builder => {
      import GraphDSL.Implicits._

      val broadcast = builder.add(Broadcast[A](2))
      val zip = builder.add(ZipWith[T, A, O]((left, right) => output(left, right)))

      broadcast.out(0) ~> processingFlow ~> zip.in0
      broadcast.out(1) ~> zip.in1

      FlowShape(broadcast.in, zip.out)
    }
    })
}
package sample.stream_shared_state

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

/**
  * Chunk a stream of strings and
  * for each line process tail elements based on the first element
  *
  * Inspired by:
  * https://discuss.lightbend.com/t/combine-prefixandtail-1-with-sink-lazysink-for-subflow-created-by-splitafter/8623
  *
  * Similar to: [[HandleFirstElementSpecially]]
  * Similar to: [[DeferredStreamCreation]]
  * Similar to: [[SplitAfter]]
  */
object SplitAfterPrefix extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()
  import system.dispatcher

  val chunkTerminator = "STOP"
  val input = Seq(
    "A", "say", "a", "word", chunkTerminator,
    "B", "be", "ready", chunkTerminator
  )

  def handleLine(prefix: Seq[String], stream: Source[String, Any]): Source[Any, Any] = {
    prefix.head match {
      case "A" =>
        stream
          .map(_.toUpperCase())
          .runFold("")(_ + " " + _)
          .onComplete(res => logger.info(s"Result for $prefix: $res"))
      case "B" =>
        stream
          .map(_.toLowerCase)
          .runFold("")(_ + " " + _)
          .onComplete(res => logger.info(s"Result for $prefix: $res"))
    }
    Source.empty
  }

  val handleChunk: Sink[String, Future[Any]] =
    Flow[String]
      .prefixAndTail(1)
      .flatMapConcat((handleLine _).tupled) // getting only a single element (of type Tuple)
      .toMat(Sink.ignore)(Keep.right)

  val done = Source(input)
    .splitAfter(_ == chunkTerminator)
    .to(handleChunk)
    .run()

  Thread.sleep(1000)
  system.terminate()
}
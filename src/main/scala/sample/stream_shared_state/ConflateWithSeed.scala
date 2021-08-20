package sample.stream_shared_state

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Source}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection._
import scala.concurrent.duration.DurationInt
import scala.util.Random

/**
  * Inspired by:
  * https://discuss.lightbend.com/t/mutable-state-in-conflatewithseed-functions/5933/3
  *
  * The operator conflateWithSeed here acts as a stateful collector/aggregator
  * of reoccurring values on slow downstream sinks
  *
  */
object ConflateWithSeed extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("ConflateWithSeed")
  implicit val executionContext = system.dispatcher

  def seed(i: Int): mutable.LinkedHashMap[Int, Int] = mutable.LinkedHashMap[Int, Int](i -> 1)

  def aggregate(state: mutable.LinkedHashMap[Int, Int], i: Int): mutable.LinkedHashMap[Int, Int] = {
    logger.info(s"Random number: $i")
    state.put(i, state.getOrElseUpdate(i, 0) + 1)
    state
  }

  // conflateWithSeed invokes the seed method every time,
  // so it is safe to materialize this flow multiple times
  val flow =
    Flow[Int]
    .map(_ => Random.nextInt(10))
    .conflateWithSeed(seed)(aggregate)

  Source(1 to 100)
    .via(flow)
    .throttle(1, 1.second) // simulate slow sink
    .runForeach(each => logger.info(s"Aggregated results: $each"))
}
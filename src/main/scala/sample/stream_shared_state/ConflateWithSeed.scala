package sample.stream_shared_state

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Source}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection._
import scala.concurrent.duration._
import scala.util.Random

/**
  * Inspired by:
  * https://discuss.lightbend.com/t/mutable-state-in-conflatewithseed-functions/5933/3
  *
  * Conflate here acts as a stateful collector/aggregator of reoccuring values
  * on slow downstream sinks
  *
  */
object ConflateWithSeed extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("ConflateWithSeed")
  implicit val executionContext = system.dispatcher

  def seed(i: Int): mutable.LinkedHashMap[Int, Int] = mutable.LinkedHashMap[Int, Int](i -> 1)

  def aggregate(state: mutable.LinkedHashMap[Int, Int], i: Int): mutable.LinkedHashMap[Int, Int] = {
    logger.info(s"Got: $i")
    state.put(i, state.getOrElseUpdate(i, 0) + 1)
    state
  }

  val lazyFlow = Flow.lazyFlow(() =>
    Flow[Int]
    .map(_ => Random.nextInt(100))
    .conflateWithSeed(seed)(aggregate)

  )
  Source(1 to 10)
    .via(lazyFlow)
    .throttle(1, 1.second) //simulate slow sink
    .runForeach(each => logger.info(s"1st reached sink: $each"))

//safe to materialize multiple times due to lazyFlow
//  Source(1 to 10)
//    .via(lazyFlow)
//    .throttle(1, 1.second) //simulate slow sink
//    .runForeach(each => logger.info(s"2nd reached sink: $each"))
}
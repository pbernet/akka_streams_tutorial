package actor

import actor.CustomCache.{CacheRequests, Devices}
import akka.actor.typed.ActorSystem
import akka.stream.scaladsl.{RestartSource, Sink, Source}

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Inspired by:
  * https://blog.colinbreck.com/rethinking-streaming-workloads-with-akka-streams-part-iii
  *
  * TODO add ask
  */
object CustomCacheRunner extends App {
  // the system is also the top level actor ref
  implicit val cache = ActorSystem[CacheRequests](CustomCache.empty, "Cache")
  implicit val ec = cache.executionContext


  val stream = RestartSource
    .withBackoff(
      minBackoff = 0.seconds,
      maxBackoff = 60.seconds,
      randomFactor = 0.1
    ) { () =>
      Source
        .tick(initialDelay = 0.seconds, interval = 2.seconds, tick = ())
        .mapAsync(parallelism = 1) { _ => getDevices}
        .map(devices => cache ! Devices(devices))
        .recover {
          case ex => cache.log.error("Failed to get devices : {}", ex)
        }
    }
    .runWith(Sink.ignore)


  def getDevices = Future(List[DeviceId](DeviceId("1"), DeviceId("2")))


}

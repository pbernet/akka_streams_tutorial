package sample.stream_actor.typed

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.AskPattern._
import akka.stream.ThrottleMode
import akka.stream.scaladsl.{RestartSource, Sink, Source}
import akka.util.Timeout
import sample.stream_actor.typed.CustomCache.{AddDevices, CacheRequests, CacheResponses, CachedDevices}

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Use typed actor as cache to show shared state and:
  *  - Request with tell from outside (= a stream)
  *  - Request-Response with ask from outside (= a stream)
  *
  * Inspired by:
  * https://blog.colinbreck.com/rethinking-streaming-workloads-with-akka-streams-part-iii
  *
  * Doc:
  * https://doc.akka.io/docs/akka/current/typed/interaction-patterns.html#request-response-with-ask-from-outside-an-actor
  *
  */
object CustomCacheRunner extends App {
  // the system is also the top level actor ref
  implicit val cache = ActorSystem[CacheRequests](CustomCache.empty, "CustomCache")
  implicit val ec = cache.executionContext
  implicit val timeout: Timeout = 5.seconds

  RestartSource
    .withBackoff(
      minBackoff = 0.seconds,
      maxBackoff = 60.seconds,
      randomFactor = 0.1
    ) { () =>
      Source
        .tick(initialDelay = 0.seconds, interval = 2.seconds, tick = ())
        .mapAsync(parallelism = 1) { _ => cache.ref.ask(ref => CustomCache.Get("42", ref)) }
        .map((each: CacheResponses) =>
          each match {
            case cachedDevices: CachedDevices => cache.log.info(s"Current amount of cached devices: ${cachedDevices.devices.size}")
            case _ => cache.log.info("No devices")
          })
        .recover {
          case ex => cache.log.error("Failed to read cached devices: ", ex)
        }
    }
    .runWith(Sink.ignore)

  val sourceOfUUID = Source(Stream.continually(java.util.UUID.randomUUID.toString).take(100))
  sourceOfUUID
    .throttle(10, 1.second, 10, ThrottleMode.shaping)
    .mapAsync(parallelism = 10)(each => Future(cache ! AddDevices(List(DeviceId(each)))))
    .runWith(Sink.ignore)
}

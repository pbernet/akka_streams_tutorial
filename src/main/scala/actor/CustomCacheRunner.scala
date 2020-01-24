package actor

import actor.CustomCache.{AddDevices, CacheRequests, CacheResponses, CachedDevices}
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.AskPattern._
import akka.stream.ThrottleMode
import akka.stream.scaladsl.{RestartSource, Sink, Source}
import akka.util.Timeout

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Use typed actor as cache to show:
  *  - Request with tell from outside (= a stream)
  *  - Request-Response with ask from outside (= a stream)
  *
  * TODO Find nice names and move to pkg stream_shared_state.actor
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
  implicit val cache = ActorSystem[CacheRequests](CustomCache.empty, "Cache")
  implicit val ec = cache.executionContext
  implicit val timeout: Timeout = 5.seconds

  val stream = RestartSource
    .withBackoff(
      minBackoff = 0.seconds,
      maxBackoff = 60.seconds,
      randomFactor = 0.1
    ) { () =>
      Source
        .tick(initialDelay = 0.seconds, interval = 2.seconds, tick = ())
        .mapAsync(parallelism = 1) { _ => cache.ref.ask(ref => CustomCache.Get("42", ref)) }

        //TODO Fails for first element (= CacheResponses.EmptyCache)
        .map((each: CacheResponses) => println(s"Current amount of cached devices: ${each.asInstanceOf[CachedDevices].devices.size}"))
        .recover {
          case ex => cache.log.error(s"Failed to read cached devices : $ex")
        }
    }
    .runWith(Sink.ignore)

  val sourceOfInt = Source(List.fill(10000)(java.util.UUID.randomUUID.toString))
  sourceOfInt
    .throttle(10, 1.second, 10, ThrottleMode.shaping)
    .mapAsync(parallelism = 10)(each => Future(cache ! AddDevices(List(DeviceId(each)))))
    .runWith(Sink.ignore)
}

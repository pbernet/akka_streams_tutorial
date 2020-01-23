package actor

import actor.SimpleCache.{CachedDeviceIds, Get}
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorSystem, Scheduler}
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}


case class DeviceId(id: String)
case class RequestId(id: String)

/**
  * Akka typed actor ask example
  *
  *
  */
object SimpleCacheRunner extends App {
  val deviceIds = List(
    DeviceId("12345"),
    DeviceId("67890"),
    DeviceId("00000")
  )

  //create and "load" with Behaviour "cached"
  val cache = ActorSystem[Get](SimpleCache.cached(deviceIds), "SimpleCache")

  implicit val timeout: Timeout             = 5.seconds
  implicit val scheduler: Scheduler         = cache.scheduler
  implicit val ec: ExecutionContextExecutor = cache.executionContext

  val requestId = RequestId("12")

  val result: Future[CachedDeviceIds] = cache.ref.ask(ref => SimpleCache.Get(requestId, ref))

  result.onComplete {
    case Success(ids) => ids.devices.foreach(println)
    case Failure(ex) => println(s"Failure: $ex")
  }
}

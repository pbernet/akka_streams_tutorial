package sample.stream_actor.typed

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}

case class DeviceId(id: String)

object CustomCache {
  sealed trait CacheRequests
  final case class Get(requestId: String, replyTo: ActorRef[CacheResponses]) extends CacheRequests
  final case class Devices(devices: List[DeviceId])                          extends CacheRequests
  final case class AddDevices(devices: List[DeviceId])                       extends CacheRequests

  sealed trait CacheResponses
  final case object EmptyCache                            extends CacheResponses
  final case class CachedDevices(devices: List[DeviceId]) extends CacheResponses

  val empty: Behavior[CacheRequests] =
    Behaviors.receive[CacheRequests] { (context, message) =>
      message match {
        case Get(requestId, replyTo) =>
          context.log.info(s"Empty cache request for requestId $requestId")
          replyTo ! EmptyCache
          Behaviors.same
        case Devices(devices) =>
          context.log.info(s"Initializing cache with: ${devices.size} devices")
          cached(devices)
        case AddDevices(devices) =>
          context.log.info(s"Initializing cache with: ${devices.size} devices")
          cached(devices)
      }
    }

  private def cached(devices: List[DeviceId]): Behavior[CacheRequests] =
    Behaviors.receive { (context, message) =>
      message match {
        case Get(requestId, replyTo) =>
          context.log.info(s"Cache request for requestId $requestId")
          replyTo ! CachedDevices(devices)
          Behaviors.same
        case Devices(updatedDevices) =>
          context.log.info(s"Updating cache with: ${updatedDevices.size} devices")
          cached(updatedDevices)
        case AddDevices(updatedDevices) =>
          context.log.info(s"Adding: ${updatedDevices.size} device(s)")
          cached(devices = devices ++ updatedDevices)
      }
    }
}

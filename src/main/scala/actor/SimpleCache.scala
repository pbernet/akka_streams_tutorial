package actor

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}

object SimpleCache {
  //request
  final case class Get(requestId: RequestId, replyTo: ActorRef[CachedDeviceIds])
  //response
  sealed trait Responses
  final case class CachedDeviceIds(devices: List[DeviceId]) extends Responses

  def cached(devices: List[DeviceId]): Behavior[Get] =
    Behaviors.receive { (context, message) =>
      context.log.info("Cache request for requestId {}.", message.requestId)
      message.replyTo ! CachedDeviceIds(devices)
      Behaviors.same
    }
}

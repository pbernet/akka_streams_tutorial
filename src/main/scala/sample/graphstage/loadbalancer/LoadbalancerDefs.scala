package sample.graphstage.loadbalancer

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.scaladsl.Flow

import scala.concurrent.duration.{FiniteDuration, _}

final case class Endpoint(host: String, port: Int) {
  override def toString = s"$host:$port"
}

sealed trait EndpointEvent {
  def endpoint: Endpoint
}

final case class EndpointUp(endpoint: Endpoint) extends EndpointEvent

final case class EndpointDown(endpoint: Endpoint) extends EndpointEvent

case object NoEndpointsAvailableException extends Exception

case object BufferOverflowException extends Exception

case object RequestsQueueClosed extends Exception

case class LoadBalancerSettings(
                                 connectionsPerEndpoint: Int,
                                 maxEndpointFailures: Int,
                                 endpointFailuresResetInterval: FiniteDuration,
                                 connectionBuilder: (Endpoint) => Flow[HttpRequest, HttpResponse, NotUsed]
                               )

case object LoadBalancerSettings {
  def default(implicit system: ActorSystem) =
    LoadBalancerSettings(
      connectionsPerEndpoint = 32,
      maxEndpointFailures = 8,
      endpointFailuresResetInterval = 5.seconds,
      (endpoint: Endpoint) => Http().outgoingConnection(endpoint.host, endpoint.port).mapMaterializedValue(_ => NotUsed)
    )
}
package sample.graphstage.loadbalancer

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._

/**
  * Stolen from:
  * https://github.com/codeheroesdev/akka-http-lb
  */
object LoadBalancer {
  def flow[T](endpointEventsSource: Source[EndpointEvent, NotUsed], settings: LoadBalancerSettings)
             (implicit system: ActorSystem, mat: ActorMaterializer) =

    Flow.fromGraph(GraphDSL.create(endpointEventsSource) { implicit builder =>
      eventsInlet =>
        import GraphDSL.Implicits._
        val lb = builder.add(new LoadBalancerStage[T](settings))
        eventsInlet ~> lb.in0
        FlowShape(lb.in1, lb.out)
    })


  def singleRequests(endpointEventsSource: Source[EndpointEvent, NotUsed], settings: LoadBalancerSettings)
                    (implicit system: ActorSystem, mat: ActorMaterializer) =
    new SingleRequestLoadBalancer(endpointEventsSource, settings)
}

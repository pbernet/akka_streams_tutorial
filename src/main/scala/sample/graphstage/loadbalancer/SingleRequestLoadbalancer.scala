package sample.graphstage.loadbalancer

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

/**
  * Stolen from:
  * https://github.com/codeheroesdev/akka-http-lb
  */
class SingleRequestLoadBalancer(endpointEventsSource: Source[EndpointEvent, NotUsed], settings: LoadBalancerSettings)
                               (implicit system: ActorSystem, mat: ActorMaterializer) {

  private val inputSource = Source.queue[(HttpRequest, Promise[HttpResponse])](1024, OverflowStrategy.dropNew)
  private val responsesSink = Sink.foreach[(Try[HttpResponse], Promise[HttpResponse])] {
    case (response, promise) => promise.completeWith(Future.fromTry(response))
  }

  private val inputQueue = inputSource
    .via(LoadBalancer.flow(endpointEventsSource, settings))
    .toMat(responsesSink)(Keep.left)
    .run()

  def request(request: HttpRequest)(implicit ec: ExecutionContext): Future[HttpResponse] = {
    val promise = Promise[HttpResponse]()
    inputQueue.offer((request, promise)).flatMap {
      case QueueOfferResult.Dropped => Future.failed(RequestsQueueClosed)
      case QueueOfferResult.QueueClosed => Future.failed(RequestsQueueClosed)
      case QueueOfferResult.Failure(ex) => Future.failed(ex)
      case QueueOfferResult.Enqueued => promise.future
    }
  }

}
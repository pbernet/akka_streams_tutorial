package sample.graphstage.loadbalancer

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpRequest, HttpResponse}
import akka.stream.scaladsl.Source
import akka.util.ByteString

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
  * Stolen from:
  * https://github.com/codeheroesdev/akka-http-lb
  */
class EndpointMock(endpoint: Endpoint) {
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  private val _processed = new AtomicInteger(0)

  private def handler(request: HttpRequest): Future[HttpResponse] = {
    _processed.incrementAndGet()
    Future {
      Thread.sleep(100)
      HttpResponse(entity = HttpEntity.CloseDelimited(ContentTypes.`application/json`, Source.single(ByteString("{}"))))
    }
  }

  Http().newServerAt(endpoint.host,  endpoint.port).bind(handler)

  def processed() = _processed.get()

  def unbind(): Unit = Await.result(system.terminate(), 5.seconds)

}

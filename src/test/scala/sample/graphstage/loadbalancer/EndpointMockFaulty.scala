package sample.graphstage.loadbalancer

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpRequest, HttpResponse}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
  * Throw random ex
  */
class EndpointMockFaulty(endpoint: Endpoint) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  private val _processed = new AtomicInteger(0)

  private def handler(request: HttpRequest): Future[HttpResponse] = {
    _processed.incrementAndGet()

    val randomTime = ThreadLocalRandom.current.nextInt(0, 5) * 100
    val start = System.currentTimeMillis()
    while ((System.currentTimeMillis() - start) < randomTime) {
      // Activate to simulate failure
      logger.info(s"RECEIVED request - Working for: $randomTime ms")
      Thread.sleep(randomTime)
      if (randomTime >= 100) throw new RuntimeException("BOOM - simulated failure on server")
    }
    Future {
      HttpResponse(entity = HttpEntity.CloseDelimited(ContentTypes.`application/json`, Source.single(ByteString("{}"))))
    }
  }

  Http().newServerAt(endpoint.host,  endpoint.port).bind(handler)

  def processed() = _processed.get()

  def unbind(): Unit = Await.result(system.terminate(), 5.seconds)

}

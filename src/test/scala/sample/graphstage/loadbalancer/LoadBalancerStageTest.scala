package sample.graphstage.loadbalancer

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, ClosedShape, OverflowStrategy}
import org.scalatest.{FlatSpec, Matchers}

import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

/**
  * Stolen from:
  * https://github.com/codeheroesdev/akka-http-lb
  */
class LoadBalancerStageTest extends FlatSpec with Matchers {
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher

  "LoadBalancerStage" should "adapt to endpoint failure" in {
    val latch = new TestLatch(10)
    val settings = LoadBalancerSettings(2, 10, 5.seconds, createConnectionBuilder(5))

    val (endpointsQueue, requestsQueue, responsesSeq) = buildLoadbalancer(latch, settings)

    endpointsQueue.offer(EndpointUp(Endpoint("localhost", 9090)))

    requestsQueue.offer((HttpRequest(), 1))
    requestsQueue.offer((HttpRequest(), 2))
    requestsQueue.offer((HttpRequest(), 3))
    requestsQueue.offer((HttpRequest(), 4))
    requestsQueue.offer((HttpRequest(), 5))
    requestsQueue.offer((HttpRequest(), 6))
    requestsQueue.offer((HttpRequest(), 7))
    requestsQueue.offer((HttpRequest(), 8))
    requestsQueue.offer((HttpRequest(), 9))
    requestsQueue.offer((HttpRequest(), 10))


    latch.await(5.seconds) shouldBe true
    requestsQueue.complete()
    val result = convertIntoStatistics(responsesSeq)

    result(1) shouldBe true
    result(2) shouldBe true
    result(3) shouldBe true
    result(4) shouldBe true
    result(5) shouldBe true
    result(6) shouldBe false
    result(7) shouldBe false
    result(8) shouldBe false
    result(9) shouldBe false
    result(10) shouldBe false

    result should have size 10
  }

  it should "adopt to endpoint up -> down" in {
    val responsesLatch = new TestLatch(5)
    val settings = LoadBalancerSettings(2, 10, 5.seconds, createConnectionBuilder(10))

    val (endpointsQueue, requestsQueue, responsesSeq) = buildLoadbalancer(responsesLatch, settings)

    endpointsQueue.offer(EndpointUp(Endpoint("localhost", 9090)))
    Thread.sleep(100)

    requestsQueue.offer((HttpRequest(), 1))
    requestsQueue.offer((HttpRequest(), 2))
    requestsQueue.offer((HttpRequest(), 3))
    requestsQueue.offer((HttpRequest(), 4))
    requestsQueue.offer((HttpRequest(), 5))

    responsesLatch.await(5.seconds) shouldBe true
    responsesLatch.reset(5)

    endpointsQueue.offer(EndpointDown(Endpoint("localhost", 9090)))
    Thread.sleep(100)

    requestsQueue.offer((HttpRequest(), 6))
    requestsQueue.offer((HttpRequest(), 7))
    requestsQueue.offer((HttpRequest(), 8))
    requestsQueue.offer((HttpRequest(), 9))
    requestsQueue.offer((HttpRequest(), 10))

    responsesLatch.await(5.seconds) shouldBe true
    requestsQueue.complete()
    val result = convertIntoStatistics(responsesSeq)

    result(1) shouldBe true
    result(2) shouldBe true
    result(3) shouldBe true
    result(4) shouldBe true
    result(5) shouldBe true
    result(6) shouldBe false
    result(7) shouldBe false
    result(8) shouldBe false
    result(9) shouldBe false
    result(10) shouldBe false

    result should have size 10

  }

  it should "adopt to endpoint up -> down -> up" in {
    val responsesLatch = new TestLatch(5)
    val settings = LoadBalancerSettings(2, 10, 5.seconds, createConnectionBuilder(10))

    val (endpointsQueue, requestsQueue, responsesSeq) = buildLoadbalancer(responsesLatch, settings)

    endpointsQueue.offer(EndpointUp(Endpoint("localhost", 9090)))
    Thread.sleep(100)

    requestsQueue.offer((HttpRequest(), 1))
    requestsQueue.offer((HttpRequest(), 2))
    requestsQueue.offer((HttpRequest(), 3))
    requestsQueue.offer((HttpRequest(), 4))
    requestsQueue.offer((HttpRequest(), 5))

    responsesLatch.await(5.seconds) shouldBe true
    responsesLatch.reset(5)

    endpointsQueue.offer(EndpointDown(Endpoint("localhost", 9090)))
    Thread.sleep(100)

    requestsQueue.offer((HttpRequest(), 6))
    requestsQueue.offer((HttpRequest(), 7))
    requestsQueue.offer((HttpRequest(), 8))
    requestsQueue.offer((HttpRequest(), 9))
    requestsQueue.offer((HttpRequest(), 10))

    responsesLatch.await(5.seconds) shouldBe true
    responsesLatch.reset(5)

    endpointsQueue.offer(EndpointUp(Endpoint("localhost", 9090)))
    Thread.sleep(100)

    requestsQueue.offer((HttpRequest(), 11))
    requestsQueue.offer((HttpRequest(), 12))
    requestsQueue.offer((HttpRequest(), 13))
    requestsQueue.offer((HttpRequest(), 14))
    requestsQueue.offer((HttpRequest(), 15))

    responsesLatch.await(5.seconds) shouldBe true
    requestsQueue.complete()
    val result = convertIntoStatistics(responsesSeq)

    result(1) shouldBe true
    result(2) shouldBe true
    result(3) shouldBe true
    result(4) shouldBe true
    result(5) shouldBe true
    result(6) shouldBe false
    result(7) shouldBe false
    result(8) shouldBe false
    result(9) shouldBe false
    result(10) shouldBe false
    result(11) shouldBe true
    result(12) shouldBe true
    result(13) shouldBe true
    result(14) shouldBe true
    result(15) shouldBe true

    result should have size 15

  }

  it should "not drop endpoint after Timeout exception" in {
    val responsesLatch = new TestLatch(10)
    val connectionBuilder = (endpoint: Endpoint) => {
      val processedRequest = new AtomicInteger(0)
      Flow[HttpRequest].map { _ =>
        val processed = processedRequest.incrementAndGet()
        if (processed == 2 || processed == 3) throw new TimeoutException() else HttpResponse()
      }
    }

    val settings = LoadBalancerSettings(1, 0, 5.seconds, connectionBuilder)

    val (endpointsQueue, requestsQueue, responsesSeq) = buildLoadbalancer(responsesLatch, settings)

    endpointsQueue.offer(EndpointUp(Endpoint("localhost", 9090)))
    Thread.sleep(100)

    requestsQueue.offer((HttpRequest(), 1))
    requestsQueue.offer((HttpRequest(), 2))
    requestsQueue.offer((HttpRequest(), 3))
    requestsQueue.offer((HttpRequest(), 4))
    requestsQueue.offer((HttpRequest(), 5))
    requestsQueue.offer((HttpRequest(), 6))
    requestsQueue.offer((HttpRequest(), 7))
    requestsQueue.offer((HttpRequest(), 8))
    requestsQueue.offer((HttpRequest(), 9))
    requestsQueue.offer((HttpRequest(), 10))

    responsesLatch.await(5.seconds) shouldBe true
    requestsQueue.complete()
    val result = convertIntoStatistics(responsesSeq)

    result(1) shouldBe true
    result(2) shouldBe false
    result(3) shouldBe false
    result(4) shouldBe true
    result(5) shouldBe true
    result(6) shouldBe true
    result(7) shouldBe true
    result(8) shouldBe true
    result(9) shouldBe true
    result(10) shouldBe true

    result should have size 10

  }


  it should "not drop endpoint if not enough failures occurs within reset interval" in {
    val latch = new TestLatch(10)
    val processedRequest = new AtomicInteger(0)

    val connectionBuilder = (endpoint: Endpoint) => {
      Flow[HttpRequest].map { _ =>
        val processed = processedRequest.incrementAndGet()
        if (Set(6, 7, 8).contains(processed)) throw new IllegalArgumentException("Failed") else HttpResponse()
      }
    }
    val settings = LoadBalancerSettings(2, 3, 250.millis, connectionBuilder)

    val (endpointsQueue, requestsQueue, responsesSeq) = buildLoadbalancer(latch, settings)

    endpointsQueue.offer(EndpointUp(Endpoint("localhost", 9090)))
    Thread.sleep(100)

    requestsQueue.offer((HttpRequest(), 1))
    requestsQueue.offer((HttpRequest(), 2))
    requestsQueue.offer((HttpRequest(), 3))
    requestsQueue.offer((HttpRequest(), 4))
    requestsQueue.offer((HttpRequest(), 5))
    requestsQueue.offer((HttpRequest(), 6))
    requestsQueue.offer((HttpRequest(), 7))

    Thread.sleep(500)

    requestsQueue.offer((HttpRequest(), 8))
    requestsQueue.offer((HttpRequest(), 9))
    requestsQueue.offer((HttpRequest(), 10))


    latch.await(5.seconds) shouldBe true
    requestsQueue.complete()
    val result = convertIntoStatistics(responsesSeq)

    result(1) shouldBe true
    result(2) shouldBe true
    result(3) shouldBe true
    result(4) shouldBe true
    result(5) shouldBe true
    result(6) shouldBe false
    result(7) shouldBe false
    result(8) shouldBe false
    result(9) shouldBe true
    result(10) shouldBe true

    result should have size 10
  }

  it should "drop endpoint if too many failures occurs within reset interval" in {
    val latch = new TestLatch(15)
    val processedRequest = new AtomicInteger(0)

    val connectionBuilder = (endpoint: Endpoint) => {
      Flow[HttpRequest].map { _ =>
        val processed = processedRequest.incrementAndGet()
        if (Set(6, 7, 8).contains(processed)) throw new IllegalStateException("Failed") else HttpResponse()
      }
    }
    val settings = LoadBalancerSettings(2, 3, 5.seconds, connectionBuilder)

    val (endpointsQueue, requestsQueue, responsesSeq) = buildLoadbalancer(latch, settings)

    endpointsQueue.offer(EndpointUp(Endpoint("localhost", 9090)))
    Thread.sleep(100)

    requestsQueue.offer((HttpRequest(), 1))
    requestsQueue.offer((HttpRequest(), 2))
    requestsQueue.offer((HttpRequest(), 3))
    requestsQueue.offer((HttpRequest(), 4))
    requestsQueue.offer((HttpRequest(), 5))
    requestsQueue.offer((HttpRequest(), 6))
    requestsQueue.offer((HttpRequest(), 7))
    requestsQueue.offer((HttpRequest(), 8))
    requestsQueue.offer((HttpRequest(), 9))
    requestsQueue.offer((HttpRequest(), 10))
    requestsQueue.offer((HttpRequest(), 11))
    requestsQueue.offer((HttpRequest(), 12))
    requestsQueue.offer((HttpRequest(), 13))
    requestsQueue.offer((HttpRequest(), 14))
    requestsQueue.offer((HttpRequest(), 15))

    latch.await(5.seconds) shouldBe true
    requestsQueue.complete()
    val result = convertIntoStatistics(responsesSeq)


    result(1) shouldBe true
    result(2) shouldBe true
    result(3) shouldBe true
    result(4) shouldBe true
    result(5) shouldBe true
    result(6) shouldBe false
    result(7) shouldBe false
    result(8) shouldBe false
    result(9) shouldBe true
    result(10) shouldBe false
    result(11) shouldBe false
    result(12) shouldBe false
    result(13) shouldBe false
    result(14) shouldBe false
    result(15) shouldBe false

    result should have size 15
  }

  it should "process all request even for small amount of connections" in {
    val latch = new TestLatch(20)
    val settings = LoadBalancerSettings(5, 10, 5.seconds, createConnectionBuilder(100))

    val (endpointsQueue, requestsQueue, responsesSeq) = buildLoadbalancer(latch, settings)

    endpointsQueue.offer(EndpointUp(Endpoint("localhost", 9090)))
    Thread sleep 100

    requestsQueue.offer((HttpRequest(), 1))
    requestsQueue.offer((HttpRequest(), 2))
    requestsQueue.offer((HttpRequest(), 3))
    requestsQueue.offer((HttpRequest(), 4))
    requestsQueue.offer((HttpRequest(), 5))
    requestsQueue.offer((HttpRequest(), 6))
    requestsQueue.offer((HttpRequest(), 7))
    requestsQueue.offer((HttpRequest(), 8))
    requestsQueue.offer((HttpRequest(), 9))
    requestsQueue.offer((HttpRequest(), 10))

    Thread sleep 500

    requestsQueue.offer((HttpRequest(), 11))
    requestsQueue.offer((HttpRequest(), 12))
    requestsQueue.offer((HttpRequest(), 13))
    requestsQueue.offer((HttpRequest(), 14))
    requestsQueue.offer((HttpRequest(), 15))
    requestsQueue.offer((HttpRequest(), 16))
    requestsQueue.offer((HttpRequest(), 17))
    requestsQueue.offer((HttpRequest(), 18))
    requestsQueue.offer((HttpRequest(), 19))
    requestsQueue.offer((HttpRequest(), 20))

    latch.await(5.seconds) shouldBe true
    requestsQueue.complete()
    val result = convertIntoStatistics(responsesSeq)

    result(1) shouldBe true
    result(2) shouldBe true
    result(3) shouldBe true
    result(4) shouldBe true
    result(5) shouldBe true
    result(6) shouldBe true
    result(7) shouldBe true
    result(8) shouldBe true
    result(9) shouldBe true
    result(10) shouldBe true
    result(11) shouldBe true
    result(12) shouldBe true
    result(13) shouldBe true
    result(14) shouldBe true
    result(15) shouldBe true
    result(16) shouldBe true
    result(17) shouldBe true
    result(18) shouldBe true
    result(19) shouldBe true
    result(20) shouldBe true


    result should have size 20
  }

  /*Helper methods*/
  private def buildLoadbalancer(latch: TestLatch, settings: LoadBalancerSettings) = {
    val endpoints = Source.queue[EndpointEvent](1024, OverflowStrategy.backpressure)
    val requests = Source.queue[(HttpRequest, Int)](1024, OverflowStrategy.backpressure)
    val responses = Flow[(Try[HttpResponse], Int)].map(result => {
      latch.countDown()
      result
    }).toMat(Sink.seq)(Keep.right)

    RunnableGraph.fromGraph(GraphDSL.create(endpoints, requests, responses)((_, _, _)) { implicit builder =>
      (endpointsIn, requestsIn, responsesOut) =>
        import GraphDSL.Implicits._
        val lb = builder.add(new LoadBalancerStage[Int](settings))
        endpointsIn ~> lb.in0
        requestsIn ~> lb.in1
        lb.out ~> responsesOut.in
        ClosedShape
    }).run()
  }

  private def createConnectionBuilder(failAfter: Int): (Endpoint) => Flow[HttpRequest, HttpResponse, NotUsed] = {
    val processedRequest = new AtomicInteger(0)
    (endpoint: Endpoint) => Flow[HttpRequest].map { _ =>
      val processed = processedRequest.incrementAndGet()
      if (processed > failAfter) throw new IllegalStateException(s"Failed") else HttpResponse()
    }
  }

  private def convertIntoStatistics(responses: Future[Seq[(Try[HttpResponse], Int)]]) =
    Await.result(responses, 5.seconds).map {
      case (Success(_), i) => i -> true
      case (Failure(ex), i) => i -> false
    }.toMap
}

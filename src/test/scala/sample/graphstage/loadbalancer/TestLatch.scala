package sample.graphstage.loadbalancer

import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.duration.Duration

/**
  * Stolen from:
  * https://github.com/codeheroesdev/akka-http-lb
  */
class TestLatch(expectedCount: Int) {
  private var _latch = new CountDownLatch(expectedCount)

  def await(duration: Duration) = _latch.await(duration.toMillis, TimeUnit.MILLISECONDS)
  def countDown() = _latch.countDown()
  def reset(value: Int) = _latch = new CountDownLatch(value)
}

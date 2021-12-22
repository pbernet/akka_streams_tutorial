package sample.loadbalancer.playground

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Balance, Sink, Source}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.ThreadLocalRandom

/**
  * PcC with using Operators
  *
  * Inspired by:
  * https://stackoverflow.com/questions/57526353/equivalent-to-balancer-broadcast-and-merge-in-pure-akka-streams
  *
  *
  * akka-http lb:
  * https://github.com/akka/akka-http/issues/3505
  *
  */
object LBWithOperators extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  // Slowest sink backpressures - for ALL

  // What about Failure: SINKS get STUCK after EX
  //  - BUG
  //  - FEATURE (when give up charachteristic is signalled)?

  // RETRY-meccano based on retryable conditions within foreach

  // Make dynamic for Single Http Requests val responseFuture = Http().singleRequest(request)

  val printSink1 = Sink.foreach[Int](each => logger.info(s"Reached sink 1: $each"))
  val printSink2 = Sink.foreach[Int](each => {
    val randomTime = ThreadLocalRandom.current.nextInt(0, 5) * 100
    logger.info(s"Reached printSink2: $each - Working for: $randomTime ms")
    val start = System.currentTimeMillis()
    Thread.sleep(randomTime)
    // Activate to simulate failure - THIS ex. is swallowed!?
    if (randomTime >= 300) {
      logger.info("about to throw ex")
      throw new RuntimeException("BOOM - simulated failure in delivery")
    } else {
      logger.info("proceed")
    }
  })


  val slowSink = Sink.foreach[Int](each => {
    val randomTime = ThreadLocalRandom.current.nextInt(0, 5) * 100
    logger.info(s"Reached slowSink: $each - Working for: $randomTime ms")
    Thread.sleep(randomTime)
    // Activate to simulate failure - THIS ex. is swallowed!?
    if (randomTime >= 300) {
      logger.info("about to throw ex")
      throw new RuntimeException("BOOM - simulated failure in delivery")
    } else {
      logger.info("proceed")
    }
  })


  val combinedSink = Sink.combine(printSink1, printSink2, slowSink)(Balance[Int](_))

  Source(1 to 1000).runWith(combinedSink)

}

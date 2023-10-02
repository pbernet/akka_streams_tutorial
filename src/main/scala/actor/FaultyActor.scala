package actor

import actor.FaultyActor.DoIt
import akkahttp.SampleRoutes.FaultyActorResponse
import org.apache.pekko.actor.{Actor, Status}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.ThreadLocalRandom

object FaultyActor {
  case class DoIt()
}

class FaultyActor extends Actor {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  var totalAttempts: Int = 0

  override def receive: Receive = {
    case DoIt() =>
      totalAttempts = totalAttempts + 1

      // TODO Add processing with Future
      // https://medium.com/@linda0511ny/error-handling-in-akka-actor-with-future-ded3da0579dd
      val randomTime = ThreadLocalRandom.current.nextInt(0, 5) * 100
      logger.info(s"Attempt: $totalAttempts - Working for: $randomTime ms")

      val start = System.currentTimeMillis()
      while ((System.currentTimeMillis() - start) < randomTime) {
        // This restarts the actor
        if (randomTime >= 300) throw new RuntimeException("BOOM - server failure")
      }
      sender() ! FaultyActorResponse(totalAttempts)
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.error(s"Failed with original failure: $reason")
    super.preRestart(reason, message)
    sender() ! Status.Failure(reason)
  }
}

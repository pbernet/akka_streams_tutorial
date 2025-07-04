package actor

import akkahttp.SampleRoutes.FaultyActorResponse
import org.apache.pekko.actor.{Actor, Status}
import org.apache.pekko.pattern.pipe
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.{ExecutionContext, Future}

object FaultyActor {
  case class DoIt()

  private case class ProcessingResult(response: FaultyActorResponse, originalSender: org.apache.pekko.actor.ActorRef)

  private case class ProcessingFailure(exception: Throwable, originalSender: org.apache.pekko.actor.ActorRef)
}

class FaultyActor extends Actor {

  import FaultyActor.*

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  var totalAttempts: Int = 0

  implicit val ec: ExecutionContext = context.dispatcher

  override def receive: Receive = {
    case DoIt() =>
      totalAttempts = totalAttempts + 1
      val currentSender = sender() // Capture sender reference

      // Process with Future and pipe result back to self
      val processingFuture = Future {
        val randomTime = ThreadLocalRandom.current.nextInt(0, 5) * 100
        logger.info(s"Attempt: $totalAttempts - Working for: $randomTime ms")

        // Simulate work with Thread.sleep instead of busy waiting
        Thread.sleep(randomTime)

        // Simulate failure condition
        if (randomTime >= 300) {
          throw new RuntimeException("BOOM - server failure")
        }

        ProcessingResult(FaultyActorResponse(totalAttempts), currentSender)
      }.recover {
        case ex => ProcessingFailure(ex, currentSender)
      }
      processingFuture.pipeTo(self)

    case ProcessingResult(response, originalSender) =>
      originalSender ! response

    case ProcessingFailure(exception, originalSender) =>
      // Send failure to original sender BEFORE throwing (which restarts the actor)
      originalSender ! Status.Failure(exception)
      // This will cause the actor to restart via supervision strategy
      throw exception
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.error(s"Actor restarting due to: $reason - totalAttempts will be reset to 0")
    super.preRestart(reason, message)
  }
}

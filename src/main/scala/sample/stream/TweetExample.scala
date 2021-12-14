package sample.stream

import akka.NotUsed
import akka.actor.{ActorSystem, Cancellable}
import akka.stream.DelayOverflowStrategy
import akka.stream.scaladsl.{Flow, MergePrioritized, Sink, Source}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.{Logger, LoggerFactory}

import java.time.{Instant, ZoneId}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Adapted tweet example from akka streams tutorial,
  * added MergePrioritized Feature
  *
  * Doc:
  * https://doc.akka.io/docs/akka/current/stream/operators/Source/combine.html
  * https://softwaremill.com/akka-2.5.4-features
  */

object TweetExample extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  final case class Author(handle: String)

  final case class Hashtag(name: String)

  final case class Tweet(author: Author, timestamp: Long, body: String) {
    def hashtags: Set[Hashtag] =
      body.split(" ").collect { case t if t.startsWith("#") => Hashtag(t) }.toSet

    override def toString = {
      val localDateTime = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).toLocalDateTime
      s"$localDateTime - ${author.handle} tweeted: ${body.take(5)}..."
    }
  }

  val akkaTag = Hashtag("#akka")

  val tweetsLowPrio: Source[Tweet, Cancellable] = Source.tick(1.second, 200.millis, NotUsed).map(_ => Tweet(Author("LowPrio"), System.currentTimeMillis, "#other #akka aBody"))
  val tweetsHighPrio: Source[Tweet, Cancellable] = Source.tick(2.second, 1.second, NotUsed).map(_ => Tweet(Author("HighPrio"), System.currentTimeMillis, "#akka #other aBody"))
  val tweetsVeryHighPrio: Source[Tweet, Cancellable] = Source.tick(2.second, 1.second, NotUsed).map(_ => Tweet(Author("VeryHighPrio"), System.currentTimeMillis, "#akka #other aBody"))

  val limitedTweets: Source[Tweet, NotUsed] = Source.combine(tweetsLowPrio, tweetsHighPrio, tweetsVeryHighPrio)(_ => MergePrioritized(List(1, 10, 100))).take(20)

  val processingFlow = Flow[Tweet]
    .filter(_.hashtags.contains(akkaTag))
    .wireTap(each => logger.info(s"$each"))

  val slowDownstream =
    Flow[Tweet]
      .delay(5.seconds, DelayOverflowStrategy.backpressure)

  val processedTweets =
    limitedTweets
      .via(processingFlow)
      .via(slowDownstream)
      .runWith(Sink.seq)

  processedTweets.onComplete {
    case Success(results) =>
      logger.info(s"Successfully processed: ${results.size} tweets")
      system.terminate()
    case Failure(exception) =>
      logger.info(s"The stream failed with: ${ExceptionUtils.getRootCause(exception)}")
      system.terminate()
  }
}

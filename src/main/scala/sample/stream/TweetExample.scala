package sample.stream

import akka.NotUsed
import akka.actor.{ActorSystem, Cancellable}
import akka.stream.scaladsl.{MergePrioritized, Sink, Source}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

/**
  * Adapted tweet example from akka streams tutorial
  * - added MergePrioritized Feature - see https://softwaremill.com/akka-2.5.4-features
  *
  */

object TweetExample {

  final case class Author(handle: String)

  final case class Hashtag(name: String)

  final case class Tweet(author: Author, timestamp: Long, body: String) {
    def hashtags: Set[Hashtag] =
      body.split(" ").collect { case t if t.startsWith("#") => Hashtag(t) }.toSet
  }

  val akkaTag = Hashtag("#akka")


  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem("TweetExample")
    implicit val ec = system.dispatcher

    val tweetsLowPrio: Source[Tweet, Cancellable]  = Source.tick(1.second, 100.millis, Tweet(Author("LowPrio"), System.currentTimeMillis, "#other #akka aBody"))
    val tweetsHighPrio: Source[Tweet, Cancellable]  = Source.tick(1.second, 1.second, Tweet(Author("HighPrio"), System.currentTimeMillis, "#akka #other aBody"))
    val tweetsVeryHighPrio: Source[Tweet, Cancellable]  = Source.tick(1.second, 1.second, Tweet(Author("VeryHighPrio"), System.currentTimeMillis, "#akka #other aBody"))

    val limitedTweets: Source[Tweet, NotUsed] = Source.combine(tweetsLowPrio, tweetsHighPrio, tweetsVeryHighPrio)(numInputs => MergePrioritized(List(1,10,100))).take(20)

    val authors: Source[Author, NotUsed] =
      limitedTweets
        .filter(_.hashtags.contains(akkaTag))
        .map(_.author)

    authors.runWith(Sink.foreach(println))

    val sumSink: Sink[Int, Future[Int]] = Sink.fold[Int, Int](0)(_ + _)
    val sum: Future[Int] = limitedTweets.map(t => 1).runWith(sumSink)
    sum.onComplete { result: Try[Int] =>
      println("Resulting Future from run completed with result: " + result)
      system.terminate()
    }
    sum.foreach(c => println(s"Number of limitedTweets processed: $c"))
  }
}

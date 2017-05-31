package tutorial

import akka.NotUsed
import akka.actor.{ActorSystem, Cancellable}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Merge, Sink, Source}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

/**
  * Akka Streams intentionally separates:
  * - linear stream structures (Flows)
  * - non-linear, branching ones (Graphs)
  */

//TODO Test the Buffer/slow computation sample
//See https://github.com/carrot-garden/scala_akka/blob/master/akka-docs/rst/scala/code/docs/stream/TwitterStreamQuickstartDocSpec.scala
object TweetGraphExample {

  final case class Author(handle: String)

  final case class Hashtag(name: String)

  final case class Tweet(author: Author, timestamp: Long, body: String) {
    def hashtags: Set[Hashtag] =
      body.split(" ").collect { case t if t.startsWith("#") => Hashtag(t) }.toSet
  }

  val akkaTag = Hashtag("#akka")


  def main(args: Array[String]): Unit = {

    implicit val system = ActorSystem("reactive-limitedTweets")
    implicit val ec = system.dispatcher
    implicit val materializer = ActorMaterializer()

    //Not used
    val tweets: Source[Tweet, NotUsed] = Source(
      Tweet(Author("rolandkuhn"), System.currentTimeMillis, "#akka rocks!") ::
        Tweet(Author("patriknw"), System.currentTimeMillis, "#akka !") ::
        Tweet(Author("bantonsson"), System.currentTimeMillis, "#akka !") ::
        Tweet(Author("drewhk"), System.currentTimeMillis, "#akka !") ::
        Tweet(Author("ktosopl"), System.currentTimeMillis, "#akka on the rocks!") ::
        Tweet(Author("mmartynas"), System.currentTimeMillis, "wow #akka !") ::
        Tweet(Author("akkateam"), System.currentTimeMillis, "#akka rocks!") ::
        Tweet(Author("bananaman"), System.currentTimeMillis, "#bananas rock!") ::
        Tweet(Author("appleman"), System.currentTimeMillis, "#apples rock!") ::
        Tweet(Author("drama"), System.currentTimeMillis, "we compared #apples to #oranges!") ::
        Nil)

    val akkaTweets: Source[Tweet, Cancellable]  = Source.tick(1.second, 1.second, Tweet(Author("me"), System.currentTimeMillis, "#akka #other aBody"))
    val otherTweets: Source[Tweet, Cancellable]  = Source.tick(1.second, 1.second, Tweet(Author("notme"), System.currentTimeMillis, "#other aBody"))
    val limitedTweets: Source[Tweet, NotUsed] = Source.combine(akkaTweets, otherTweets)(Merge(_)).take(10)

    val authors: Source[Author, NotUsed] =
      limitedTweets
        .filter(_.hashtags.contains(akkaTag))
        .map(_.author)
    authors.runWith(Sink.foreach(println))

    val hashtags: Source[Hashtag, NotUsed] = limitedTweets.mapConcat(_.hashtags.toList)
    // hashtags.runWith(Sink.foreach(println))


    val sumSink: Sink[Int, Future[Int]] = Sink.fold[Int, Int](0)(_ + _)
    val sum: Future[Int] = limitedTweets.map(t => 1).runWith(sumSink)


    sum.onComplete { (result: Try[Int]) =>
      println("Resulting Future from 1st run completed with result: " + result)
    }

    sum.foreach(c => println(s"Result 1st run limitedTweets processed: $c"))

    Thread.sleep(2000)

    val sum2: Future[Int] = limitedTweets.map(t => 1).runWith(sumSink)
    sum2.onComplete { (result: Try[Int]) =>
      println("Resulting Future from 2nd run completed with result: " + result)
      system.terminate()
    }
  }
}

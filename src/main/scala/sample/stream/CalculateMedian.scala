package sample.stream

import java.util.concurrent.ThreadLocalRandom

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, ThrottleMode}

import scala.annotation.tailrec
import scala.concurrent.duration._

/**
  * Inspired by:
  * https://stackoverflow.com/questions/4662292/scala-median-implementation
  *
  * TODO
  * To calculate the "all time median of medians grouped by 5" we would need to store the values (eg in an actor)
  *
  */
//noinspection LanguageFeature
object CalculateMedian {
  implicit val system = ActorSystem("CalculateMedian")
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  def main(args: Array[String]) = {
    val maxRandomNumber = 100
    val source = Source.fromIterator(() => Iterator.continually(ThreadLocalRandom.current().nextDouble(maxRandomNumber)))

    source
      .throttle(1, 10.millis, 1, ThrottleMode.shaping)
      .groupedWithin(100, 1.second)
      //.map{each => println(each); each}
      .map(each => medianOfMedians(each.toArray))
      .runForeach(result => println(s"Median of Median (grouped by 5) over the last 100 elements: $result"))
      .onComplete(_ => system.terminate())
  }

  @tailrec def findKMedian(arr: Array[Double], k: Int)(implicit choosePivot: Array[Double] => Double): Double = {
    val a = choosePivot(arr)
    val (s, b) = arr partition (a >)
    if (s.length == k) a
    // The following test is used to avoid infinite repetition
    else if (s.isEmpty) {
      val (s, b) = arr partition (a ==)
      if (s.length > k) a
      else findKMedian(b, k - s.length)
    } else if (s.length < k) findKMedian(b, k - s.length)
    else findKMedian(s, k)
  }

  def medianUpTo5(five: Array[Double]): Double = {
    def order2(a: Array[Double], i: Int, j: Int) = {
      if (a(i) > a(j)) {
        val t = a(i); a(i) = a(j); a(j) = t
      }
    }

    def pairs(a: Array[Double], i: Int, j: Int, k: Int, l: Int) = {
      if (a(i) < a(k)) {
        order2(a, j, k); a(j)
      }
      else {
        order2(a, i, l); a(i)
      }
    }

    if (five.length < 2) {
      return five(0)
    }
    order2(five, 0, 1)
    if (five.length < 4) return if (five.length == 2 || five(2) < five(0)) five(0)
    else if (five(2) > five(1)) five(1)
    else five(2)
    order2(five, 2, 3)
    if (five.length < 5) pairs(five, 0, 1, 2, 3)
    else if (five(0) < five(2)) {
      order2(five, 1, 4); pairs(five, 1, 4, 2, 3)
    }
    else {
      order2(five, 3, 4); pairs(five, 0, 1, 3, 4)
    }
  }

  def medianOfMedians(arr: Array[Double]): Double = {
    val medians = arr grouped 5 map medianUpTo5 toArray;
    if (medians.length <= 5) medianUpTo5(medians)
    else medianOfMedians(medians)
  }
}

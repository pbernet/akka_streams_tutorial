package tools

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class SubtitleTranslatorSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private implicit val system: ActorSystem = ActorSystem("SubtitleTranslatorSpec")

  override def afterAll(): Unit = Await.result(system.terminate(), 5.seconds)

  private def block(start: Long, end: Long, line: String = "x"): SubtitleBlock =
    SubtitleBlock(start, end, Seq(line))

  private def group(maxGap: Int, blocks: SubtitleBlock*): Seq[List[SubtitleBlock]] =
    Await.result(
      Source(blocks.toList).via(SubtitleTranslator.groupByScene(maxGap)).runWith(Sink.seq),
      5.seconds)

  test("empty input yields no scenes") {
    group(maxGap = 1000) shouldBe empty
  }

  test("single block yields one scene with that block") {
    val b = block(0, 1000)
    group(maxGap = 1000, b) shouldBe Seq(List(b))
  }

  test("blocks within the gap are grouped into one scene") {
    val b1 = block(0, 1000)
    val b2 = block(1500, 2000) // gap = 500 ms < 1000 ms
    group(maxGap = 1000, b1, b2) shouldBe Seq(List(b1, b2))
  }

  test("blocks beyond the gap split into separate scenes") {
    val b1 = block(0, 1000)
    val b2 = block(3000, 4000) // gap = 2000 ms >= 1000 ms
    group(maxGap = 1000, b1, b2) shouldBe Seq(List(b1), List(b2))
  }

  test("gap exactly equal to maxGap starts a new scene") {
    val b1 = block(0, 1000)
    val b2 = block(2000, 3000) // gap = 1000 ms, condition is strict `<`
    group(maxGap = 1000, b1, b2) shouldBe Seq(List(b1), List(b2))
  }

  test("groups three close blocks into one scene") {
    val b1 = block(0, 500)
    val b2 = block(800, 1200)
    val b3 = block(1500, 2000)
    group(maxGap = 1000, b1, b2, b3) shouldBe Seq(List(b1, b2, b3))
  }

  test("groups three blocks into two scenes") {
    val b1 = block(0, 1000)
    val b2 = block(5000, 6000) // gap = 4000 ms -> closes first scene
    val b3 = block(6500, 7000)
    group(maxGap = 1000, b1, b2, b3) shouldBe Seq(List(b1), List(b2, b3))
  }

  test("respects larger maxGap") {
    val b1 = block(0, 1000)
    val b2 = block(3000, 4000) // gap = 2000 ms < 5000 ms
    group(maxGap = 5000, b1, b2) shouldBe Seq(List(b1, b2))
  }
}

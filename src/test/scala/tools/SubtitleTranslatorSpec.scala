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

  private val scene = List(
    SubtitleBlock(60000, 62000, Seq("Where are you going?")),
    SubtitleBlock(62300, 63500, Seq("Home.")),
    SubtitleBlock(63800, 65000, Seq("Wait for me!")))

  private val sceneTranslated = Seq(
    (60000, 62000, "Wohin gehst du?"),
    (62300, 63500, "Nach Hause."),
    (63800, 65000, "Warte auf mich!"))

  private val correctResponse = "Wohin gehst du?\n\nNach Hause.\n\nWarte auf mich!"
  // The LLM merged "Home." into the first line
  private val tooFewLinesResponse = "Wohin gehst du? Nach Hause.\n\nWarte auf mich!"
  // The LLM added a preamble despite the output rules
  private val tooManyLinesResponse = "Hier ist die Übersetzung:\n\n" + correctResponse

  private def translate(defaultResponse: String, fallbackModel: String => String): Seq[(Long, Long, String)] =
    SubtitleTranslator.translateAligned(scene, "prompt", _ => defaultResponse, fallbackModel)
      .map { case (b, line) => (b.start, b.end, line) }

  test("translate: one line per block from default model is used, fallback model is not called") {
    translate(correctResponse, _ => fail("fallback model must not be called")) shouldBe sceneTranslated
  }

  test("translate: fewer lines from default model fall back to fallback model") {
    translate(tooFewLinesResponse, _ => correctResponse) shouldBe sceneTranslated
  }

  test("translate: more lines from default model fall back to fallback model") {
    translate(tooManyLinesResponse, _ => correctResponse) shouldBe sceneTranslated
  }

  test("translate: no plausible translation from both models keeps the original scene") {
    translate(tooFewLinesResponse, _ => tooManyLinesResponse) shouldBe Seq(
      (60000, 62000, "Where are you going?"),
      (62300, 63500, "Home."),
      (63800, 65000, "Wait for me!"))
  }
}

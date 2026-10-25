package tools

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path}

class SrtParserSpec extends AnyFunSuite with Matchers {

  private val sourceFilePath = "src/main/resources/EN_challenges.srt"

  private def tempPath(prefix: String): Path = {
    val p = Files.createTempFile(prefix, ".srt")
    p.toFile.deleteOnExit()
    p
  }

  private def tempSrtWith(content: String): String = {
    val p = tempPath("tmp")
    Files.writeString(p, content)
    p.toString
  }

  test("parse valid srt file") {
    val blocks = new SrtParser(sourceFilePath).runSync()

    blocks.nonEmpty shouldBe true
    blocks.head.lines.nonEmpty shouldBe true
  }

  test("convert subtitle block to formatted output") {
    val formatted = SubtitleBlock(start = 1000, end = 4000, lines = Seq("First line", "Second line"))
      .formatOutBlock(1)

    formatted should include("00:00:01,000 --> 00:00:04,000")
    formatted should include("First line")
    formatted should include("Second line")
  }

  test("parse subtitle block with multiple lines") {
    SubtitleBlock(start = 0, end = 2000, lines = Seq("Line 1", "Line 2", "Line 3"))
      .allLines shouldBe "Line 1 Line 2 Line 3"
  }

  test("format time correctly") {
    val formatted = SubtitleBlock(start = 3661000, end = 3662000, lines = Seq("Test"))
      .formatOutBlock(1)

    formatted should include("01:01:01,000 --> 01:01:02,000")
  }

  test("handle empty subtitle file") {
    new SrtParser(tempSrtWith("")).runSync() shouldBe empty
  }

  test("handle invalid srt file format") {
    val src = tempSrtWith(
      """
        |Invalid Format
        |No timestamps here
        |Just random text
        |Without proper structure
        |---> wrong separator
    """.stripMargin)

    assertThrows[java.time.format.DateTimeParseException] {
      new SrtParser(src).runSync()
    }
  }

  test("parse srt file with LF or CRLF line endings, independent of platform") {
    def srtWith(nl: String) =
      Seq("1", "00:00:01,000 --> 00:00:04,000", "First line", "Second line", "",
        "2", "00:00:05,500 --> 00:00:06,000", "Last line", "").mkString(nl)

    val expected = Seq(
      SubtitleBlock(1000, 4000, Seq("First line", "Second line")),
      SubtitleBlock(5500, 6000, Seq("Last line")))

    new SrtParser(tempSrtWith(srtWith("\n"))).runSync() shouldBe expected
    new SrtParser(tempSrtWith(srtWith("\r\n"))).runSync() shouldBe expected
  }

  test("timeShift forward shifts all timestamps by positive offset") {
    assertTimeShift(5000L)
  }

  test("timeShift backward shifts all timestamps by negative offset") {
    assertTimeShift(-1000L)
  }

  test("timeShift helper writes shifted timestamps to target file, clamped at zero") {
    val src = tempSrtWith(
      "1\n00:00:01,000 --> 00:00:02,000\nHello\n\n" +
        "2\n00:59:58,800 --> 00:59:59,900\nWorld\n")

    def shifted(shiftBy: Long) = {
      val target = tempPath(s"helper_shifted_$shiftBy")
      SrtParser.timeShift(src, target.toString, shiftBy)
      Files.readString(target)
    }

    shifted(1500) shouldBe
      "1\n00:00:02,500 --> 00:00:03,500\nHello\n\n" +
        "2\n01:00:00,300 --> 01:00:01,400\nWorld\n\n"

    shifted(-1500) shouldBe
      "1\n00:00:00,000 --> 00:00:00,500\nHello\n\n" +
        "2\n00:59:57,300 --> 00:59:58,400\nWorld\n\n"
  }

  private def assertTimeShift(shiftBy: Long): Unit = {
    val target = tempPath(s"shifted_$shiftBy")
    new SrtParser(sourceFilePath).timeShift(target.toString, shiftBy)

    val shifted = new SrtParser(target.toString).runSync()
    val original = new SrtParser(sourceFilePath).runSync()

    shifted.size shouldBe original.size
    shifted.zip(original).foreach { case (s, o) =>
      s.start shouldBe Math.max(0, o.start + shiftBy)
      s.end shouldBe Math.max(0, o.end + shiftBy)
      s.lines shouldBe o.lines
    }
  }
}

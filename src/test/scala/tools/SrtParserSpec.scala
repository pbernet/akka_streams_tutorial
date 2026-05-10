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
    val p = tempPath("invalid")
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

  test("timeShift forward shifts all timestamps by positive offset") {
    assertTimeShift(5000L)
  }

  test("timeShift backward shifts all timestamps by negative offset") {
    assertTimeShift(-1000L)
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

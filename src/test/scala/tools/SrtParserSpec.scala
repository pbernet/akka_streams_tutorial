package tools

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.PrintWriter
import java.nio.file.Files

class SrtParserSpec extends AnyFunSuite with Matchers {

  test("parse valid srt file") {
    val parser = new SrtParser("src/main/resources/EN_challenges.srt")
    val blocks = parser.runSync()

    blocks.nonEmpty shouldBe true
    blocks.head.lines.nonEmpty shouldBe true
  }

  test("convert subtitle block to formatted output") {
    val block = SubtitleBlock(
      start = 1000,
      end = 4000,
      lines = Seq("First line", "Second line")
    )
    val formatted = block.formatOutBlock(1)

    formatted should include("00:00:01,000 --> 00:00:04,000")
    formatted should include("First line")
    formatted should include("Second line")
  }

  test("parse subtitle block with multiple lines") {
    val block = SubtitleBlock(
      start = 0,
      end = 2000,
      lines = Seq("Line 1", "Line 2", "Line 3")
    )
    block.allLines shouldBe "Line 1 Line 2 Line 3"
  }

  test("format time correctly") {
    val block = SubtitleBlock(
      start = 3661000, // 1 hour, 1 minute, 1 second
      end = 3662000, // 1 hour, 1 minute, 2 seconds
      lines = Seq("Test")
    )
    val formatted = block.formatOutBlock(1)

    formatted should include("01:01:01,000 --> 01:01:02,000")
  }


  test("handle empty subtitle file") {
    val tempFile = Files.createTempFile("empty", ".srt")
    tempFile.toFile.deleteOnExit()
    val parser = new SrtParser(tempFile.toString)
    val blocks = parser.runSync()

    blocks shouldBe empty
  }

  test("handle invalid srt file format") {
    val tempFile = Files.createTempFile("invalid", ".srt")
    tempFile.toFile.deleteOnExit()
    val writer = new PrintWriter(tempFile.toFile)
    writer.write(
      """
        |Invalid Format
        |No timestamps here
        |Just random text
        |Without proper structure
        |---> wrong separator
    """.stripMargin)
    writer.close()

    val parser = new SrtParser(tempFile.toString)

    assertThrows[java.time.format.DateTimeParseException] {
      parser.runSync()
    }
  }
}
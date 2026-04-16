package tools

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.PrintWriter
import java.nio.file.Files

class SrtParserSpec extends AnyFunSuite with Matchers {

  private val sourceFilePath = "src/main/resources/EN_challenges.srt"

  test("parse valid srt file") {
    val parser = new SrtParser(sourceFilePath)
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

  test("timeShift forward shifts all timestamps by positive offset") {
    val targetFile = Files.createTempFile("shifted_forward", ".srt")
    targetFile.toFile.deleteOnExit()
    val shiftBy = 5000L // 5 seconds forward

    val parser = new SrtParser(sourceFilePath)
    parser.timeShift(targetFile.toString, shiftBy)

    val shiftedParser = new SrtParser(targetFile.toString)
    val shiftedBlocks = shiftedParser.runSync()

    val originalParser = new SrtParser(sourceFilePath)
    val originalBlocks = originalParser.runSync()

    shiftedBlocks.size shouldBe originalBlocks.size
    shiftedBlocks.zip(originalBlocks).foreach { case (shifted, original) =>
      shifted.start shouldBe original.start + shiftBy
      shifted.end shouldBe original.end + shiftBy
      shifted.lines shouldBe original.lines
    }
  }

  test("timeShift backward shifts all timestamps by negative offset") {
    val targetFile = Files.createTempFile("shifted_backward", ".srt")
    targetFile.toFile.deleteOnExit()
    val shiftBy = -1000L // 1 second backward

    val parser = new SrtParser(sourceFilePath)
    parser.timeShift(targetFile.toString, shiftBy)

    val shiftedParser = new SrtParser(targetFile.toString)
    val shiftedBlocks = shiftedParser.runSync()

    val originalParser = new SrtParser(sourceFilePath)
    val originalBlocks = originalParser.runSync()

    shiftedBlocks.size shouldBe originalBlocks.size
    shiftedBlocks.zip(originalBlocks).foreach { case (shifted, original) =>
      shifted.start shouldBe Math.max(0, original.start + shiftBy)
      shifted.end shouldBe Math.max(0, original.end + shiftBy)
      shifted.lines shouldBe original.lines
    }
  }
}
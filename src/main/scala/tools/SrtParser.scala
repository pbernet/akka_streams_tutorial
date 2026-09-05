package tools

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{FileIO, Framing, Sink, Source, StreamConverters}
import org.apache.pekko.util.ByteString
import org.slf4j.{Logger, LoggerFactory}

import java.io.FileInputStream
import java.nio.file.Paths
import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalTime}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor}

/**
  * Run with a reasonably well formatted .srt file in UTF-8 encoding
  *
  * @param sourceFilePath
  */
class SrtParser(sourceFilePath: String) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val ls: String = sys.props("line.separator")

  private val timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss,SSS")

  private val frameByEmptyLine = Framing.delimiter(
    ByteString(ls + ls),
    maximumFrameLength = 2048,
    allowTruncation = true)

  private def toMillisOfDay(value: String) =
    LocalTime.parse(value, timeFormatter).toNanoOfDay / 1_000_000

  private def convertTo(raw: String) = {
    val parts = raw.split(ls)
    val times = parts(1).split("""\s*-->\s*""")
    SubtitleBlock(toMillisOfDay(times(0)), toMillisOfDay(times(1)), parts.drop(2).toList)
  }

  val source: Source[SubtitleBlock, Any] =
    StreamConverters.fromInputStream(() => new FileInputStream(sourceFilePath))
      .via(frameByEmptyLine)
      .map(_.utf8String)
      .map(convertTo)

  def runSync(): Seq[SubtitleBlock] = {
    val result = Await.result(source.runWith(Sink.seq), 10.seconds)
    system.terminate()
    result
  }

  /**
    * Shift all timestamps by the given offset and write the result to targetFilePath.
    *
    * @param targetFilePath path of the output .srt file
    * @param shiftBy        offset in milliseconds (positive = forward, negative = backward)
    */
  def timeShift(targetFilePath: String, shiftBy: Long): Unit = {
    val ioResult = Await.result(
      source
        .map(b => b.copy(start = Math.max(0, b.start + shiftBy), end = Math.max(0, b.end + shiftBy)))
        .zipWithIndex
        .map { case (block, idx) => ByteString(block.formatOutBlock(idx + 1), "UTF-8") }
        .runWith(FileIO.toPath(Paths.get(targetFilePath))),
      10.seconds)
    system.terminate()
    logger.info(s"Wrote: ${ioResult.count} bytes to: $targetFilePath")
  }
}

object SrtParser {
  def apply(sourceFilePath: String): SrtParser = new SrtParser(sourceFilePath)

  def timeShift(sourceFilePath: String, targetFilePath: String, shiftBy: Long): Unit =
    new SrtParser(sourceFilePath).timeShift(targetFilePath, shiftBy)

  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    val parser = new SrtParser("src/main/resources/EN_challenges.srt")
    val result = parser.runSync()
    logger.info(s"File contains: ${result.size} SubtitleBlock(s)")
    logger.info(s"Blocks: $result")

  }
}

case class SubtitleBlock(start: Long, end: Long, lines: Seq[String]) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val endLineTag = "\n" // for openAI API
  val ls: String = sys.props("line.separator") // for file IO

  def allLines: String = lines.mkString(" ")

  def allLinesEnd: String = allLines + endLineTag + endLineTag

  def formatOutBlock(blockCounter: Long): String = {
    // Spec: https://wiki.videolan.org/SubRip
    val out = s"$blockCounter$ls$startTime --> $endTime$ls${lines.mkString("\n")}$ls$ls"
    logger.info(s"Writing block:$ls {}", out)
    out
  }

  def startTime: String = toTime(start)

  def endTime: String = toTime(end)

  private def toTime(ms: Long) = {
    val d = Duration.ofMillis(ms)
    f"${d.toHoursPart}%02d:${d.toMinutesPart}%02d:${d.toSecondsPart}%02d,${d.toMillisPart}%03d"
  }
}

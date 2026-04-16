package tools

import org.slf4j.{Logger, LoggerFactory}

/**
  * Shift all subtitle timestamps of an .srt file forward or backward.
  *
  * Usage:
  * Put source file in project root
  * sbt "runMain tools.SrtShifter source.srt shifted.srt 5000"
  *
  * Example forward  (+5 s): SrtShifter input.srt output.srt 5000
  * Example backward (-3 s): SrtShifter input.srt output.srt -3000
  */
object SrtShifter extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val defaultSource = "src/main/resources/EN_challenges.srt"
  val defaultTarget = "output.srt"
  val defaultShift = 5000L

  val sourceFilePath = if (args.length > 0) args(0) else defaultSource
  val targetFilePath = if (args.length > 1) args(1) else defaultTarget
  val shiftBy = if (args.length > 2) args(2).toLong else defaultShift

  if (args.length < 3) {
    logger.info(s"Using defaults: sourceFilePath=$sourceFilePath, targetFilePath=$targetFilePath, shiftBy=$shiftBy")
  }

  logger.info(s"Shifting: '$sourceFilePath' by: $shiftBy ms -> '$targetFilePath'")
  val parser = new SrtParser(sourceFilePath)
  parser.timeShift(targetFilePath, shiftBy)
}

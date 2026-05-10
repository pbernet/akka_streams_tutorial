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

  val sourceFilePath = if (args.length > 0) args(0) else "src/main/resources/EN_challenges.srt"
  val targetFilePath = if (args.length > 1) args(1) else "output.srt"
  val shiftBy = if (args.length > 2) args(2).toLong else 5000L

  if (args.length < 3)
    logger.info(s"Using defaults: sourceFilePath=$sourceFilePath, targetFilePath=$targetFilePath, shiftBy=$shiftBy")

  logger.info(s"Shifting: '$sourceFilePath' by: $shiftBy ms -> '$targetFilePath'")
  new SrtParser(sourceFilePath).timeShift(targetFilePath, shiftBy)
}

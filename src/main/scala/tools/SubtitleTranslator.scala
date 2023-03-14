package tools

import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, Flow, Source}
import akka.stream.{IOResult, ThrottleMode}
import akka.util.ByteString
import com.crowdscriber.caption.common.Vocabulary.{Srt, SubtitleBlock}
import com.crowdscriber.caption.srtdissector.SrtDissector
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}

import java.io.FileInputStream
import java.nio.file.Paths
import java.time.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

/**
  * Translate all blocks of an English .srt file to a target lang using OpenAI API
  *
  * Workflow:
  *  - Load all blocks from the .srt source file
  *  - Split blocks to scenes (= all blocks in a session window), depending on maxGapSeconds
  *  - Translate all blocks of a scene in one prompt (one line per block) via the openAI API
  *  - Continuously write translated blocks to target file
  *
  * Works with these OpenAI API endpoints:
  *  - Default:  /chat/completions (gpt-3.5-turbo)    https://platform.openai.com/docs/guides/chat/chat-vs-completions
  *  - Fallback: /completions      (text-davinci-003) https://beta.openai.com/docs/api-reference/completions/create
  */
object SubtitleTranslator extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val sourceFilePath = "src/main/resources/EN_challenges.srt"
  val targetFilePath = "DE_challenges.srt"
  val targetLanguage = "German"

  val maxGapSeconds = 1 // time between scenes (session windows)
  val endBlockTag = "\n" // one block per line
  var totalTokensUsed = 0

  // Extension methods for SubtitleBlock
  implicit class SubtitleBlockExt(block: SubtitleBlock) {
    def allLines: String = block.lines.mkString(" ")

    def allLinesEbNewLine: String = allLines + endBlockTag + endBlockTag
  }

  // Source file must be in utf-8
  val srt: Try[Srt] = SrtDissector(new FileInputStream(sourceFilePath))
  logger.info("Number of subtitleBlocks to translate: {}", srt.get.length)

  private val dummyLastElement = Source.single(SubtitleBlock(0, 0, List("")))
  val source = Source.fromIterator(() => srt.get.iterator) ++ dummyLastElement

  val workflow = Flow[SubtitleBlock]
    .via(partitionToScenes(maxGapSeconds))
    .map(translateScene)

  val fileSink = FileIO.toPath(Paths.get(targetFilePath))
  val processingSink = Flow[SubtitleBlock]
    .statefulMapConcat(addBlockCounter())
    .map { case (block: SubtitleBlock, blockCounter: Int) =>
      ByteString(formatOutBlock(block, blockCounter))
    }
    .toMat(fileSink)((_, bytesWritten) => bytesWritten)

  val done = source
    // https://platform.openai.com/docs/guides/rate-limits/overview
    .throttle(25, 60.seconds, 25, ThrottleMode.shaping)
    .via(workflow)
    .mapConcat(identity) // flatten
    .runWith(processingSink)

  terminateWhen(done)


  private def partitionToScenes(maxGap: Int) = {
    Flow[SubtitleBlock]
      .sliding(2) // allows to compare this element with the next element
      .statefulMapConcat(hasGap(maxGap)) // stateful decision
      .splitAfter(_._2) // split when gap has been reached
      .map(_._1) // proceed with payload
      //.wireTap(each => println(s"Scene block:\n$each"))
      .fold(Vector.empty[SubtitleBlock])(_ :+ _)
      .mergeSubstreams
  }

  private def hasGap(maxGap: Int): () => Seq[SubtitleBlock] => Iterable[(SubtitleBlock, Boolean)] = {
    () => {
      slidingElements => {
        if (slidingElements.size == 2) {
          val current = slidingElements.head.end
          val next = slidingElements.tail.head.start
          val gap = next - current
          List((slidingElements.head, gap >= maxGap * 1000))
        } else {
          List((slidingElements.head, false))
        }
      }
    }
  }

  private def translateScene(sceneOrig: Vector[SubtitleBlock]) = {
    logger.info(s"About to translate scene with: ${sceneOrig.size} original blocks")

    val allLines = sceneOrig.foldLeft("")((acc, block) => acc + block.allLinesEbNewLine)
    val toTranslate = generatePrompt(allLines)
    logger.info(s"Request prompt: $toTranslate")

    // First try with the cheaper model
    var translated = new OpenAICompletions().runChatCompletions(toTranslate)

    if (!isTranslationPlausible(translated.getLeft, sceneOrig.size)) {
      // Fallback to the more reliable model
      translated = new OpenAICompletions().runCompletions(toTranslate)
    }

    val newTokens = translated.getRight
    totalTokensUsed = totalTokensUsed + newTokens

    val rawResponseText = translated.getLeft
    logger.info("Response text: {}", rawResponseText)
    val seed: Vector[SubtitleBlock] = Vector.empty

    val sceneTranslatedManually: Vector[SubtitleBlock] =
      rawResponseText
        .split(endBlockTag)
        .filterNot(each => each.isEmpty)
        .zipWithIndex
        .foldLeft(seed) { (acc: Vector[SubtitleBlock], rawResponseTextSplit: (String, Int)) =>
          val massagedResult = massageResultText(rawResponseTextSplit._1)
          val origBlock =
            if (sceneOrig.isDefinedAt(rawResponseTextSplit._2)) {
              sceneOrig(rawResponseTextSplit._2)
            } else {
              // Root cause: Not plausible translation, eg added lines at beginning or at end.
              // TODO This workaround could corrupt .srt files because of duplicate timestamp entries
              logger.warn(s"This should not happen: sceneOrig has size: ${sceneOrig.size} but access to element: ${rawResponseTextSplit._2} requested. Fallback to last original block")
              sceneOrig.last
            }
          val translatedBlock = origBlock.copy(lines = massagedResult)
          logger.info(s"Translation: ${translatedBlock.allLines} $totalTokensUsed(+$newTokens)")
          acc.appended(translatedBlock)
        }
    logger.info(s"Finished line by line translation of scene with: ${sceneTranslatedManually.size} blocks")
    sceneTranslatedManually
  }

  private def isTranslationPlausible(rawResponseText: String, originalSize: Int) = {
    val resultSize = rawResponseText
      .split(endBlockTag)
      .filterNot(each => each.isEmpty)
      .length

    resultSize == originalSize
  }

  private def generatePrompt(text: String) = {
    s"""
       |Translate the text lines below from English to $targetLanguage.
       |
       |Desired format:
       |<line separated list of translated lines, honor line breaks>
       |
       |Text lines:
       |$text
       |
       |""".stripMargin
  }

  private def massageResultText(text: String): List[String] = {
    val textCleaned = clean(text)
    // Two people conversation in one block
    if (textCleaned.startsWith("-")) {
      textCleaned.split("-").map(x => "-" + x).toList.tail
    }
    else {
      splitSentence(textCleaned)
    }
  }

  private def clean(text: String): String = {
    val filtered = text.filter(_ >= ' ')
    if (filtered.startsWith("\"")) filtered.substring(1, filtered.length() - 1)
    else filtered
  }

  private def splitSentence(text: String): List[String] = {
    val maxCharPerLine = 40

    if (text.length > maxCharPerLine && text.contains(",")) {
      val indexFirstComma = text.indexOf(",")
      val offset = 15
      // Comma must not be at beginning or at end
      if (indexFirstComma > offset && indexFirstComma < text.length - offset)
        List(text.substring(0, indexFirstComma + 1), text.substring(indexFirstComma + 1, text.length))
      else splitSentenceHonorWords(text)
    }
    else if (text.length > maxCharPerLine) {
      splitSentenceHonorWords(text)
    } else {
      List(text)
    }
  }

  private def splitSentenceHonorWords(sentence: String) = {
    val words = sentence.split(" ")
    val mid = words.length / 2
    val firstHalf = words.slice(0, mid).mkString(" ")
    val secondHalf = words.slice(mid, words.length).mkString(" ")
    List(firstHalf, secondHalf)
  }

  private def toTime(ms: Int): String = {
    val d = Duration.ofMillis(ms)
    val hours = StringUtils.leftPad(d.toHoursPart.toString, 2, "0")
    val minutes = StringUtils.leftPad(d.toMinutesPart.toString, 2, "0")
    val seconds = StringUtils.leftPad(d.toSecondsPart.toString, 2, "0")
    val milliSeconds = StringUtils.leftPad(d.toMillisPart.toString, 3, "0")
    s"$hours:$minutes:$seconds,$milliSeconds"
  }

  private def addBlockCounter() = {
    () => {
      var counter = 0
      block: SubtitleBlock =>
        block match {
          case block: SubtitleBlock =>
            counter = counter + 1
            List((block, counter))
        }
    }
  }

  private def formatOutBlock(block: SubtitleBlock, blockCounter: Int) = {
    val ls = sys.props("line.separator")
    // Spec: https://wiki.videolan.org/SubRip
    val outputFormatted = s"$blockCounter$ls${toTime(block.start)} --> ${toTime(block.end)}$ls${block.lines.mkString("\n")}$ls$ls"
    logger.info(s"Writing block:$ls {}", outputFormatted)
    outputFormatted
  }

  def terminateWhen(done: Future[IOResult]) = {
    done.onComplete {
      case Success(_) =>
        println(s"Flow Success. Translated to target file: $targetFilePath About to terminate...")
        system.terminate()
      case Failure(e) =>
        println(s"Flow Failure: $e. Partial results are in target file:$targetFilePath About to terminate...")
        system.terminate()
    }
  }
}

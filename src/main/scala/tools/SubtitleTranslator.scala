package tools

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{FileIO, Flow, Source}
import org.apache.pekko.stream.{IOResult, ThrottleMode}
import org.apache.pekko.util.ByteString
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.Paths
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

/**
  * Translate all blocks of an English .srt file to a target lang using LLMs via LangChain4j
  *
  * Workflow:
  *  - Load all blocks from the .srt source file with [[SrtParser]]
  *  - Group blocks to scenes (= all blocks within a session window), depending on `maxGap`
  *  - Translate all blocks of a scene in one prompt (one line per block) via the LLM API
  *  - Continuously write translated blocks to target file
  *
  * Usage:
  *  - Wire Params, e.g. sourceFilePath
  *  - Decide, whether to use optional context to streamline translation
  *  - Add API_KEY in [[AnthropicCompletions]], [[OpenAICompletions]] and then run this class
  *  - Scan log for WARN log messages and improve corresponding blocks in target file manually
  *
  * Remarks:
  *  - Numerical block headers in the .srt files are not interpreted, only timestamps matter
  *
  * Similar to: [[sample.stream.SessionWindow]]
  */
object SubtitleTranslator {
  private[tools] def groupByScene(maxGap: Int) =
    Flow[SubtitleBlock].statefulMap(() => List.empty[SubtitleBlock])(
      (scene, next) => {
        val gapMs = next.start - scene.lastOption.getOrElse(next).end
        if (gapMs < maxGap) (scene :+ next, Nil)
        else (List(next), scene)
      },
      scene => Some(scene)
    ).filter(_.nonEmpty)

  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    implicit val system: ActorSystem = ActorSystem()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    // Params
    val sourceFilePath = "src/main/resources/EN_challenges.srt"
    val targetFilePath = "DE_challenges.srt"
    val targetLanguage = "German"

    // Optional context
    val useContext = false
    val movieTitle = "Bob Marley - One Love"
    val movieReleaseYear = 2024

    val defaultModel = if (useContext) OpenAICompletions.withContext(movieTitle, movieReleaseYear) else new OpenAICompletions()
    val fallbackModel = if (useContext) AnthropicCompletions.withContext(movieTitle, movieReleaseYear) else new AnthropicCompletions()

    // Tuning params
    val maxGap = 1000 // gap time in ms between two scenes (= session windows)
    val endLineTag = "\n"
    val maxCharPerTranslatedLine = 40
    val conversationPrefix = "-"

    // Meta info
    var totalTokensUsed = 0

    // Ensure that all blocks are readable before translation starts
    val parseResult = SrtParser(sourceFilePath).runSync()
    logger.info("Number of subtitleBlocks to translate: {}", parseResult.length)

    val processingSink = Flow[SubtitleBlock]
      .zipWithIndex
      .map { case (block, i) => ByteString(block.formatOutBlock(i + 1)) }
      .toMat(FileIO.toPath(Paths.get(targetFilePath)))((_, bytesWritten) => bytesWritten)

    lazy val done = Source(parseResult)
      // https://platform.openai.com/settings/organization/limits
      .throttle(100, 1.minute, 25, ThrottleMode.shaping)
      .via(groupByScene(maxGap))
      .map(translateScene)
      .mapConcat(identity)
      .runWith(processingSink)

    terminateWhen(done)

    def translateScene(sceneOrig: List[SubtitleBlock]) = {
      logger.info(s"About to translate scene with: ${sceneOrig.size} original blocks targeting: $defaultModel")

      val toTranslate = generateTranslationPrompt(sceneOrig.map(_.allLinesEnd).mkString)
      logger.info(s"Translation prompt: $toTranslate")

      val firstShot = defaultModel.runCompletions(toTranslate)
      val translated =
        if (isTranslationPlausible(firstShot.getLeft, sceneOrig.size)) firstShot
        else {
          logger.info(s"Translation with: $defaultModel is not plausible, lines do not match. Fallback to: $fallbackModel")
          fallbackModel.runCompletions(toTranslate)
        }

      totalTokensUsed += translated.getRight
      logger.debug("Response text: {}", translated.getLeft)

      val sceneTranslated = nonEmptyLines(translated.getLeft).zipWithIndex.map { case (line, i) =>
        val origBlock =
          if (sceneOrig.isDefinedAt(i)) sceneOrig(i)
          else {
            // Root cause: No plausible translation e.g. due to added lines at beginning or at end of response
            logger.warn(s"This should not happen: sceneOrig has size: ${sceneOrig.size} but access to element: $i requested. Fallback to last original block")
            sceneOrig.last
          }
        val block = origBlock.copy(lines = massageResultText(line))
        logger.info(s"Translated block to: ${block.allLines}")
        block
      }.toVector

      logger.info(s"Finished translation of scene with: ${sceneTranslated.size} blocks")
      sceneTranslated
    }

    def nonEmptyLines(text: String): Array[String] =
      text.split(endLineTag).filterNot(_.isEmpty)

    def isTranslationPlausible(rawResponseText: String, originalSize: Int) =
      nonEmptyLines(rawResponseText).length == originalSize

    def generateTranslationPrompt(text: String) =
      s"""
         |Translate the text lines below from English to $targetLanguage.
         |
         |Desired format:
         |<line separated list of translated text lines, honor all line breaks>
         |
         |Text lines:
         |$text
         |
         |Strict output rules:
         |- Return ONLY the translated text, nothing else.
         |- In doubt return the original text.
         |
         |""".stripMargin

    def generateShortenPrompt(text: String) = {
      s"""
         |Rewrite the text below to ${maxCharPerTranslatedLine * 2 - 10} characters at most, keeping the original language.
         |
         |Strict output rules:
         |- Return ONLY the rewritten text, nothing else.
         |- No preamble, explanation, labels, options, quotes, markdown, or trailing notes.
         |- Output exactly one single line.
         |
         |Text:
         |$text
         |
         |""".stripMargin
    }

    def massageResultText(text: String) = {
      val cleaned = clean(text)
      if (isConversation(cleaned)) splitConversation(cleaned)
      else if (isTextTooLong(cleaned)) shortenLongText(cleaned)
      else splitSentence(cleaned)
    }

    def isConversation(text: String): Boolean =
      text.startsWith(s"$conversationPrefix ")

    def splitConversation(text: String): List[String] =
      // Split so that words like "Mm-hmm" are preserved
      text.split(s" (?=$conversationPrefix )").toList

    def isTextTooLong(text: String): Boolean =
      text.length > maxCharPerTranslatedLine * 2 + 10

    def shortenLongText(text: String): List[String] = {
      logger.warn(s"Translated block text is too long (${text.length} chars). Try to shorten via API call. Check result manually")
      val toShorten = generateShortenPrompt(text)
      logger.info(s"Shorten prompt: $toShorten")
      val responseShort = new AnthropicCompletions().runCompletions(toShorten)
      splitSentence(clean(responseShort.getLeft))
    }

    def clean(text: String) = {
      // Replace control chars (\n, \r, \t) with a space so words on adjacent
      // lines do not get glued together, then collapse runs of whitespace
      val normalized = text.map(c => if (c < ' ') ' ' else c).replaceAll("\\s+", " ").trim
      if (normalized.startsWith("\"") && normalized.endsWith("\"")) normalized.substring(1, normalized.length - 1)
      else normalized
    }

    def splitSentence(text: String) = {
      if (text.length > maxCharPerTranslatedLine && text.contains(",")) {
        val commaIdx = text.indexOf(",")
        val offset = 15
        if (commaIdx > offset && commaIdx < text.length - offset)
          List(text.substring(0, commaIdx + 1), text.substring(commaIdx + 1))
        else splitSentenceHonorWords(text)
      }
      else if (text.length > maxCharPerTranslatedLine) splitSentenceHonorWords(text)
      else List(text)
    }

    def splitSentenceHonorWords(sentence: String) = {
      val words = sentence.split(" ")
      val mid = words.length / 2
      List(words.take(mid).mkString(" "), words.drop(mid).mkString(" "))
    }

    def terminateWhen(done: Future[IOResult]): Unit = done.onComplete {
      case Success(_) =>
        logger.info(s"Flow Success. Finished writing to target file: $targetFilePath. Around $totalTokensUsed tokens used. About to terminate...")
        system.terminate()
      case Failure(e) =>
        logger.info(s"Flow Failure: $e. Partial translations are in target file: $targetFilePath About to terminate...")
        system.terminate()
    }
  }
}

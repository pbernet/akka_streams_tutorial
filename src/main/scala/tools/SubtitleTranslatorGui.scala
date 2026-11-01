package tools

import io.circe.Encoder
import io.circe.generic.auto.*
import io.circe.parser.decode
import io.circe.syntax.*
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.marshalling.sse.EventStreamMarshalling.*
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.sse.ServerSentEvent
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.ActorAttributes
import org.apache.pekko.stream.scaladsl.Source
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import scala.collection.immutable.ListMap
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.sys.process.Process
import scala.util.{Failure, Success, Try}

/**
  * Run [[SubtitleTranslator]] with HTML GUI, which supports:
  *  - Translation: drop an English .srt file, choose target language and optional movie context, follow the status
  *  - Post-translation editing: edit the translated text of all blocks scene by scene, each edit is saved to the target file
  *
  * The .srt target file with the translation is written to the working dir, e.g. DE_myMovie.srt
  *
  * Usage:
  *  - Set env vars OPENAI_API_KEY / ANTHROPIC_API_KEY
  *  - Run this class
  *  - Browse to: http://localhost:8090/subtitles
  */
object SubtitleTranslatorGui {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val languages: ListMap[String, String] =
    ListMap("DE" -> "German", "EN" -> "English", "FR" -> "French", "IT" -> "Italian", "ES" -> "Spanish")

  private case class LogEntry(time: String, level: String, message: String)

  private type Phase = "idle" | "preparing" | "translating" | "done" | "failed"

  private given Encoder[Phase] = Encoder.encodeString.contramap(identity)

  private case class Status(phase: Phase = "idle", fileName: String = "", targetLanguage: String = "",
                            movie: String = "", blocksTotal: Int = 0, scenesTotal: Int = 0, scenesDone: Int = 0,
                            tokensUsed: Int = 0, targetFile: String = "", log: Vector[LogEntry] = Vector.empty)

  private case class BlockView(number: Int, startTime: String, endTime: String, original: Seq[String],
                               translated: Seq[String], shortened: Boolean)

  private case class SceneView(startTime: String, endTime: String, fallbackUsed: Boolean, keptOriginal: Boolean,
                               blocks: Seq[BlockView])

  private case class EditRequest(lines: Seq[String])

  private case class State(status: Status = Status(), scenes: Vector[TranslatedScene] = Vector.empty, targetPath: Option[Path] = None)

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem()
    import system.dispatcher

    val port = 8090
    val url = s"http://localhost:$port/subtitles"
    Http().newServerAt("localhost", port).bind(new SubtitleTranslatorGui(SubtitleTranslator.apply).route).onComplete {
      case Success(_) =>
        logger.info(s"Subtitle translator web interface listening at: $url")
        if (System.getProperty("os.name").toLowerCase == "mac os x") Process(s"open $url").!
      case Failure(e) =>
        logger.error(s"Failed to bind to port: $port. About to terminate...", e)
        system.terminate()
    }
  }
}

/**
  * Holds the state of one translation job at a time
  *
  * @param createTranslator (target language, optional movie (title, release year)) => translator
  */
class SubtitleTranslatorGui(createTranslator: (String, Option[(String, Int)]) => SubtitleTranslator)(implicit system: ActorSystem) {

  import SubtitleTranslatorGui.{*, given}

  private val blockingEc: ExecutionContext = system.dispatchers.lookup("pekko.actor.default-blocking-io-dispatcher")
  private val timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss")

  // Shared by HTTP request threads, the job futures and the translation stream, hence only accessed via read/modify
  private var state = State()

  private def read[A](f: State => A): A = synchronized(f(state))

  /** Atomic read-modify-write. Side effects of `f`, e.g. writes to the target file, happen in the order of the state changes */
  private def modify[A](f: State => (State, A)): A = synchronized {
    val (next, result) = f(state)
    state = next
    result
  }

  private def updateStatus(change: Status => Status): Unit =
    modify(s => (s.copy(status = change(s.status)), ()))

  val route: Route =
    pathPrefix("subtitles") {
      concat(
        pathEndOrSingleSlash {
          get {
            getFromFile(Paths.get("src/main/resources/subtitletranslator.html").toFile, ContentTypes.`text/html(UTF-8)`)
          }
        },
        path("translate") {
          post {
            entity(as[Multipart.FormData]) { formData =>
              onSuccess(formData.toStrict(10.seconds)) { strict =>
                val parts = strict.strictParts.map(part => part.name -> part).toMap
                val fileName = parts.get("file").flatMap(_.filename).map(Paths.get(_).getFileName.toString)
                val langCode = parts.get("targetLanguage").map(_.entity.data.utf8String).getOrElse("DE")
                val movieTitle = parts.get("movieTitle").map(_.entity.data.utf8String.trim).filter(_.nonEmpty)
                val movieYear = parts.get("movieReleaseYear").flatMap(p => Try(p.entity.data.utf8String.trim.toInt).toOption)
                (fileName, languages.get(langCode)) match {
                  case (None, _) => complete(StatusCodes.BadRequest, "Missing .srt file")
                  case (_, None) => complete(StatusCodes.BadRequest, s"Unknown target language: $langCode")
                  case (Some(_), Some(_)) if movieTitle.isDefined && movieYear.isEmpty =>
                    complete(StatusCodes.BadRequest, "Movie context needs title and release year")
                  case (Some(name), Some(_)) =>
                    val movie = movieTitle.zip(movieYear)
                    if (startJob(name, parts("file").entity.data.toArray, langCode, movie)) complete(StatusCodes.Accepted)
                    else complete(StatusCodes.Conflict, "A translation is already running")
                }
              }
            }
          }
        },
        path("events") {
          get {
            complete {
              Source.tick(0.seconds, 300.millis, NotUsed)
                .map(_ => read(_.status).asJson.noSpaces)
                .statefulMap(() => "")((last, json) => (json, Option.when(json != last)(json)), _ => None)
                .collect { case Some(json) => ServerSentEvent(json) }
                .keepAlive(10.seconds, () => ServerSentEvent.heartbeat)
            }
          }
        },
        path("scenes") {
          get {
            complete(HttpEntity(ContentTypes.`application/json`, sceneViews.asJson.noSpaces))
          }
        },
        path("scenes" / IntNumber / "blocks" / IntNumber) { (sceneIdx, blockIdx) =>
          put {
            entity(as[String]) { body =>
              decode[EditRequest](body).map(_.lines.map(_.trim).filter(_.nonEmpty)) match {
                case Left(e) => complete(StatusCodes.BadRequest, e.getMessage)
                case Right(lines) if lines.isEmpty => complete(StatusCodes.BadRequest, "Translated text must not be empty")
                case Right(lines) =>
                  editBlock(sceneIdx, blockIdx, lines) match {
                    case Success(_) => complete(StatusCodes.NoContent)
                    case Failure(e) =>
                      logger.warn(s"Failed to save edit of block: $blockIdx in scene: $sceneIdx", e)
                      complete(StatusCodes.Conflict, e.getMessage)
                  }
              }
            }
          }
        }
      )
    }

  private def startJob(fileName: String, bytes: Array[Byte], langCode: String, movie: Option[(String, Int)]): Boolean = {
    val canStart = modify { s =>
      if (Set[Phase]("preparing", "translating").contains(s.status.phase)) (s, false)
      else (State(Status(phase = "preparing", fileName = fileName, targetLanguage = languages(langCode),
        movie = movie.map((title, year) => s"$title ($year)").getOrElse(""))), true)
    }
    if (canStart) runJob(fileName, bytes, langCode, movie)
    canStart
  }

  private def runJob(fileName: String, bytes: Array[Byte], langCode: String, movie: Option[(String, Int)]): Unit = {
    implicit val ec: ExecutionContext = blockingEc

    Future {
      val blocks = SrtParser(bytes).runSync()
      if (blocks.isEmpty) throw new IllegalArgumentException(s"No subtitle blocks found in: $fileName")
      val scenesTotal = SubtitleTranslator.countScenes(blocks, SubtitleTranslator.maxGap)
      val target = uniqueTargetPath(s"${langCode}_$fileName")
      Files.writeString(target, "", UTF_8)
      modify(s => (s.copy(targetPath = Some(target), status = s.status.copy(blocksTotal = blocks.size,
        scenesTotal = scenesTotal, targetFile = target.toAbsolutePath.toString)), ()))
      log("info", s"Parsed: ${blocks.size} blocks into $scenesTotal scenes")
      movie.foreach((title, year) => log("info", s"Generating movie context for: $title ($year)..."))
      val translator = createTranslator(languages(langCode), movie)
      movie.foreach(_ => log("info", "Movie context generated"))
      (blocks, translator)
    }.flatMap { (blocks, translator) =>
      updateStatus(_.copy(phase = "translating"))
      log("info", s"Translating to: ${languages(langCode)} with default model: ${translator.defaultModel}, fallback model: ${translator.fallbackModel}")
      translator.translate(blocks)
        .addAttributes(ActorAttributes.dispatcher(ActorAttributes.IODispatcher.dispatcher))
        .runForeach(scene => onSceneTranslated(scene, translator.totalTokensUsed))
        .map(_ => translator.totalTokensUsed)
    }.onComplete {
      case Success(tokensUsed) =>
        updateStatus(_.copy(phase = "done", tokensUsed = tokensUsed))
        log("info", s"Finished translation. Around: $tokensUsed tokens used. Edit the translated text below")
      case Failure(e) =>
        updateStatus(_.copy(phase = "failed"))
        log("error", s"Translation failed: ${e.getMessage}. Partial translations are in the target file", e)
    }
  }

  private def onSceneTranslated(scene: TranslatedScene, tokensUsed: Int): Unit = {
    val (sceneNumber, firstBlockNumber) = modify { s =>
      val firstBlockNumber = s.scenes.map(_.blocks.size).sum + 1
      // Append like the CLI does, so that partial translations are kept on failure
      val srt = scene.blocks.zipWithIndex.map { case ((_, translated), i) => translated.toSrt(firstBlockNumber + i) }.mkString
      Files.writeString(s.targetPath.get, srt, UTF_8, StandardOpenOption.APPEND)
      val scenes = s.scenes :+ scene
      (s.copy(scenes = scenes, status = s.status.copy(scenesDone = scenes.size, tokensUsed = tokensUsed)), (scenes.size, firstBlockNumber))
    }

    val where = s"Scene $sceneNumber (${scene.blocks.head._1.startTime})"
    if (scene.keptOriginal) log("warn", s"$where: no plausible translation, kept original text")
    else if (scene.fallbackUsed) log("info", s"$where: translated with fallback model")
    scene.shortened.toSeq.sorted.foreach(i => log("warn", s"$where: block ${firstBlockNumber + i} was too long and got shortened"))
  }

  private def editBlock(sceneIdx: Int, blockIdx: Int, lines: Seq[String]): Try[Unit] = Try {
    modify { s =>
      if (s.status.phase != "done") throw new IllegalStateException("Editing is possible when the translation is done")
      val scene = s.scenes(sceneIdx)
      val (orig, translated) = scene.blocks(blockIdx)
      val scenes = s.scenes.updated(sceneIdx, scene.copy(blocks = scene.blocks.updated(blockIdx, (orig, translated.copy(lines = lines)))))
      val srt = scenes.flatMap(_.blocks.map(_._2)).zipWithIndex.map((block, i) => block.toSrt(i + 1)).mkString
      Files.writeString(s.targetPath.get, srt, UTF_8)
      logger.info(s"Saved edit of block: ${scenes.take(sceneIdx).map(_.blocks.size).sum + blockIdx + 1} to: ${s.targetPath.get}")
      (s.copy(scenes = scenes), ())
    }
  }

  private def sceneViews: Seq[SceneView] = {
    val scenes = read(_.scenes)
    val firstBlockNumbers = scenes.scanLeft(1)(_ + _.blocks.size)
    scenes.zip(firstBlockNumbers).map { (scene, first) =>
      SceneView(scene.blocks.head._1.startTime, scene.blocks.last._1.endTime, scene.fallbackUsed, scene.keptOriginal,
        scene.blocks.zipWithIndex.map { case ((orig, translated), i) =>
          BlockView(first + i, orig.startTime, orig.endTime, orig.lines, translated.lines, scene.shortened.contains(i))
        })
    }
  }

  private def log(level: String, message: String, cause: Throwable = null): Unit = {
    level match {
      case "info" => logger.info(message)
      case "warn" => logger.warn(message)
      case _ => logger.error(message, cause)
    }
    updateStatus(s => s.copy(log = s.log :+ LogEntry(LocalTime.now.format(timeFormatter), level, message)))
  }

  /** Do not overwrite existing files, e.g. earlier translations */
  private def uniqueTargetPath(name: String): Path = {
    val (base, ext) = name.lastIndexOf('.') match {
      case -1 => (name, "")
      case i => (name.substring(0, i), name.substring(i))
    }
    Iterator.from(0)
      .map(i => Paths.get(if (i == 0) name else s"${base}_$i$ext"))
      .find(!Files.exists(_))
      .get
  }
}

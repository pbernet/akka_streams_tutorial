package alpakka.file.uploader

import java.nio.file.{FileSystems, Files, Path, StandardCopyOption}

import akka.actor.ActorSystem
import akka.stream.alpakka.file.DirectoryChange
import akka.stream.alpakka.file.scaladsl.{Directory, DirectoryChangesSource}
import akka.stream.scaladsl.Sink
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.DurationInt

/**
  * Pick up files from directory in `uploadDir` on:
  *  - startup
  *  - when new files are added
  *
  * do a HTTP file upload via [[Uploader]]
  * and finally move them to `processedDir`
  *
  * TODO
  *  - Combine the two sources with merge operator
  *  - Pass actor system to Uploader
  *  - Handle not happy path scenarios
  */
object DirectoryListener extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("DirectoryListener")
  implicit val executionContext = system.dispatcher

  val uploader = Uploader()

  val fs = FileSystems.getDefault
  val rootDir = fs.getPath("./uploader")
  val uploadDir = rootDir.resolve("upload")
  val processedDir = rootDir.resolve("processed")

  Files.createDirectories(rootDir)
  Files.createDirectories(uploadDir)
  Files.createDirectories(processedDir)

  sourceDirInitial()
  sourceDirChanges()

  def sourceDirInitial() = {
    Directory.ls(uploadDir)
      .mapAsync(1)(each => {
        logger.info(s"Source file initial: $each")
        uploadAndMove(each)
      })
      .runWith(Sink.ignore)
  }

  def sourceDirChanges() = {
    val changes = DirectoryChangesSource(uploadDir, pollInterval = 1.second, maxBufferSize = 1000)
    changes.runForeach {
      case (path, change: DirectoryChange) =>
        if (change == DirectoryChange.Creation) {
          logger.info("Source file listener: " + path + ", Change: " + change)
          uploadAndMove(path)
        }
    }
  }

  private def uploadAndMove(each: Path) = {
    uploader.upload(each.toFile).andThen { case _ => move(each) }
  }

  private def move(sourcePath: Path): Unit = {
    val fileToMove = processedDir.resolve(sourcePath.getFileName)
    Files.move(sourcePath, fileToMove, StandardCopyOption.REPLACE_EXISTING)
  }
}
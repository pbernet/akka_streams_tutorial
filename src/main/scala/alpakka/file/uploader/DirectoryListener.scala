package alpakka.file.uploader

import akka.actor.ActorSystem
import akka.stream.alpakka.file.DirectoryChange
import akka.stream.alpakka.file.scaladsl.{Directory, DirectoryChangesSource}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.{FileSystems, Files, Path, StandardCopyOption}
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

/**
  * Pick up (new/changed) files in the directory `uploadDir`
  * Do a HTTP file upload via [[Uploader]]
  * Finally move the file to `processedDir`
  *
  * Remarks
  *  - DirectoryChangesSource does not work for files in sub folders
  *  - Similar example: https://akka.io/alpakka-samples/file-to-elasticsearch/index.html
  */
object DirectoryListener extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("DirectoryListener")
  implicit val executionContext = system.dispatcher

  val uploader = Uploader(system)

  val fs = FileSystems.getDefault
  val rootDir = fs.getPath("uploader")
  val uploadDir = rootDir.resolve("upload")
  val uploadSubDir = uploadDir.resolve("subdir")
  val processedDir = rootDir.resolve("processed")

  Files.createDirectories(rootDir)
  Files.createDirectories(uploadDir)
  Files.createDirectories(uploadSubDir)
  Files.createDirectories(processedDir)

  uploadAllFilesFromSourceDir()

  def uploadAllFilesFromSourceDir() = {
    logger.info(s"About to start listening for changes in `uploadDir`: $uploadDir")
    DirectoryChangesSource(uploadDir, pollInterval = 1.second, maxBufferSize = 1000)
      // Detect changes in *this* dir
      .collect { case (path, DirectoryChange.Creation) => path }
      // Merge with files found on startup
      .merge(Directory.ls(uploadDir))
      .mapAsync(1)(path => uploadAndMove(path))
      .run()
  }

  private def uploadAndMove(path: Path) = {
    if (path.toFile.isFile) {
      logger.info(s"About to upload and move file: $path")
      uploader.upload(path.toFile).andThen { case _ => move(path) }
    } else {
      Future.successful("Do nothing on dir")
    }
  }

  private def move(sourcePath: Path): Unit = {
    val targetPath = processedDir.resolve(sourcePath.getFileName)
    Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING)
  }
}
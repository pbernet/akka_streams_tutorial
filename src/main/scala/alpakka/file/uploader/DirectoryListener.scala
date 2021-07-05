package alpakka.file.uploader

import akka.actor.ActorSystem
import akka.stream.alpakka.file.DirectoryChange
import akka.stream.alpakka.file.scaladsl.{Directory, DirectoryChangesSource}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file._
import scala.compat.java8.StreamConverters.StreamHasToScala
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

/**
  * Pick up (new/changed) files in the directory structure under `./uploader/upload`
  * Do a HTTP file upload via [[Uploader]]
  * Finally move the file to `./uploader/processed`
  *
  * Remarks:
  *  - Currently files are detected in root dir as well as in subdirs
  *  - However, added dirs at runtime are currently not detected,
  *    so if a "dir with files" is dropped at runtime, the dir is not detected
  *  - Does probably not generate events for CIFS mounted shares either
  *
  *  Inspired by:
  *  https://discuss.lightbend.com/t/using-directorychangessource-recursively/7630
  *
  * Ideas:
  *  - Workaround: Restart Files.walk(uploadDir) periodically
  *  - Find a way to get the changed event from the "dir with files"
  *  - Use RecursiveDirectoryChangesSource from
  *    https://github.com/sysco-middleware/alpakka-connectors
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

  Files
    .walk(uploadDir)
    .filter(path => Files.isDirectory(path))
    .toScala[List]
    .map(path => uploadAllFilesFromSourceDir(path))

  def uploadAllFilesFromSourceDir(path: Path) = {
    logger.info(s"About to start listening for changes in dir: $path")
    DirectoryChangesSource(path, pollInterval = 1.second, maxBufferSize = 1000)
      // Detect changes in this dir
      .collect { case (path, DirectoryChange.Creation) => path}
      // Merge with files found on startup
      .merge(Directory.ls(path))
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
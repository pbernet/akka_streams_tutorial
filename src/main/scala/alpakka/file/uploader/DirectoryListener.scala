package alpakka.file.uploader

import akka.actor.ActorSystem
import akka.stream.alpakka.file.DirectoryChange
import akka.stream.alpakka.file.scaladsl.{Directory, DirectoryChangesSource}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.{FileSystems, Files, Path, StandardCopyOption}
import scala.concurrent.duration.DurationInt

/**
  * Pick up files from directory in `uploadDir` on:
  *  - startup
  *  - when new files are added
  *
  * do a HTTP file upload via [[Uploader]]
  * and finally move them to `processedDir`
  *
  * Similar example (regarding directory listening):
  * https://akka.io/alpakka-samples/file-to-elasticsearch/index.html
  *
  * TODO
  *  - Handle not happy path scenarios
  */
object DirectoryListener extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("DirectoryListener")
  implicit val executionContext = system.dispatcher

  val uploader = Uploader(system)

  val fs = FileSystems.getDefault
  val rootDir = fs.getPath("./uploader")
  val uploadDir = rootDir.resolve("upload")
  val processedDir = rootDir.resolve("processed")

  Files.createDirectories(rootDir)
  Files.createDirectories(uploadDir)
  Files.createDirectories(processedDir)

  uploadAllFilesFromSourceDir()

  def uploadAllFilesFromSourceDir() = {
    DirectoryChangesSource(uploadDir, pollInterval = 1.second, maxBufferSize = 1000)
       // Files added to the dir
      .collect { case (path, DirectoryChange.Creation) => path }
       // Include files encountered on startup
      .merge(Directory.ls(uploadDir))
      .mapAsync(1)(path => {
        logger.info(s"Upload and move: $path")
        uploadAndMove(path)
      })
      .run()
  }

  private def uploadAndMove(each: Path) = {
    uploader.upload(each.toFile).andThen { case _ => move(each) }
  }

  private def move(sourcePath: Path): Unit = {
    val targetPath = processedDir.resolve(sourcePath.getFileName)
    Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING)
  }
}
package alpakka.file.uploader

import org.apache.commons.io.monitor.{FileAlterationListenerAdaptor, FileAlterationMonitor, FileAlterationObserver}
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.{ActorSystem, Terminated}
import org.apache.pekko.stream.connectors.file.scaladsl.Directory
import org.apache.pekko.stream.scaladsl.{Keep, MergeHub, Sink, Source}
import org.apache.pekko.stream.{ActorAttributes, Supervision}
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.nio.file.*
import scala.compat.java8.StreamConverters.StreamHasToScala
import scala.concurrent.Future

/**
  * Detect (new/changed) files in `rootDir/upload` and send file paths to the upload sink
  * From the upload sink do a HTTP file upload via [[Uploader]]
  * Finally move the file to `rootDir/processed`
  *
  * Run with test class: [[DirectoryWatcherSpec]]
  * Run with gatling : [[DirectoryWatcherSimulation]]
  *
  * Remarks:
  *  - [[FileAlterationListenerAdaptor]] allows to recursively listen to file changes at runtime
  *  - Currently Pekko connectors [[DirectoryChangesSource]] can not do this, see:
  *    https://discuss.lightbend.com/t/using-directorychangessource-recursively/7630
  *  - Alternative Impl: https://github.com/gmethvin/directory-watcher
  */
class DirectoryWatcher(uploadDir: Path, processedDir: Path) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val uploader: Uploader = Uploader(system)

  if (!Files.exists(uploadDir)) {
    val errorMessage = s"Invalid upload directory path: $uploadDir"
    logger.error(errorMessage)
    throw new IllegalArgumentException(errorMessage)
  }

  private val uploadSink: Sink[Path, NotUsed] = MergeHub
    .source[Path](perProducerBufferSize = 1000)
    .mapAsync(1)(path => uploadAndMove(path))
    .withAttributes(ActorAttributes.supervisionStrategy(Supervision.restartingDecider))
    .to(Sink.ignore)
    .run()

  // Handle initial files in dir structure
  handleInitialFiles(uploadDir)

  // Handle recursively added/changed files at runtime
  handleChangedFiles(uploadDir)

  private def handleInitialFiles(uploadDirPath: Path) = {
    Files
      .walk(uploadDirPath)
      .filter(path => Files.isDirectory(path))
      .toScala[List]
      .map(path => uploadAllFilesFrom(path))
  }

  private def uploadAllFilesFrom(path: Path) = {
    logger.info(s"About to upload files in dir: $path")

    Directory.ls(path)
      .runWith(uploadSink)
  }

  private def handleChangedFiles(uploadDirPath: Path) = {
    logger.info(s"About to start listening for file changes in dir: $uploadDirPath")
    val observer = FileAlterationObserver.builder().setPath(uploadDirPath).get()
    val monitor = new FileAlterationMonitor(1000)
    val listener = new FileAlterationListenerAdaptor() {

      override def onFileCreate(file: File): Unit = {
        logger.info(s"CREATED: $file")
        addToUploadQueue(file.toPath: Path)
      }

      override def onFileDelete(file: File): Unit = {
        logger.info(s"DELETED: $file")
      }

      override def onFileChange(file: File): Unit = {
        logger.info(s"CHANGED: $file")
      }
    }
    observer.addListener(listener)
    monitor.addObserver(observer)
    monitor.start()
    monitor
  }

  private def addToUploadQueue(path: Path): Unit = {
    Source.single(path)
      .watchTermination(Keep.right)
      .toMat(uploadSink)(Keep.left)
      .run()
      .failed
      .foreach(exception => logger.warn(s"Offer failed for: $path", exception))
  }

  private def uploadAndMove(path: Path) = {
    if (Files.exists(path) && path.toFile.isFile && Files.isReadable(path)) {
      logger.info(s"About to upload and move file: $path")
      val response = uploader.upload(path.toFile).andThen { case _ => move(path) }
      logger.info(s"Successfully uploaded and moved file: $path")
      response
    } else {
      val msg = s"Do nothing for: $path (because unreadable file or dir)"
      logger.info(msg)
      Future.successful(msg)
    }
  }

  private def move(sourcePath: Path): Unit = {
    val targetPath = processedDir.resolve(sourcePath.getFileName)
    Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING)
  }

  def countFilesProcessed(): Int = {
    new File(processedDir.toString).list().length
  }

  def stop(): Future[Terminated] = {
    logger.info("About to shutdown DirectoryWatcher/Uploader...")
    uploader.stop()
  }
}

object DirectoryWatcher {
  def main(args: Array[String]): Unit = {}
  def apply(uploadDir: Path, processedDir: Path) = new DirectoryWatcher(uploadDir, processedDir)
}

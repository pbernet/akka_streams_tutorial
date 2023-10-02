package alpakka.file.uploader

import org.apache.commons.io.monitor.{FileAlterationListenerAdaptor, FileAlterationMonitor, FileAlterationObserver}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.connectors.file.scaladsl.Directory
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.stream.{OverflowStrategy, QueueOfferResult}
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.nio.file._
import scala.compat.java8.StreamConverters.StreamHasToScala
import scala.concurrent.Future

/**
  * Detect (new/changed) files in `rootDir/upload` and send file path to uploadSourceQueue
  * From uploadSourceQueue do a HTTP file upload via [[Uploader]]
  * Finally move the file to `rootDir/processed`
  *
  * Run with test class: [[DirectoryListenerSpec]]
  *
  * Remarks:
  *  - [[FileAlterationListenerAdaptor]] allows to recursively listen to file changes at runtime
  *  - Currently Alpakka DirectoryChangesSource can not do this, see:
  *    https://discuss.lightbend.com/t/using-directorychangessource-recursively/7630
  *  - Alternative Impl: https://github.com/gmethvin/directory-watcher
  */
class DirectoryListener(uploadDir: Path, processedDir: Path) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val uploader = Uploader(system)

  val uploadSourceQueue = Source
    .queue[Path](bufferSize = 1000, OverflowStrategy.backpressure, maxConcurrentOffers = 1000)
    .mapAsync(1)(path => uploadAndMove(path))
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
      .mapAsync(1)(each => addToUploadQueue(each))
      .run()
  }

  private def handleChangedFiles(uploadDirPath: Path) = {
    logger.info(s"About to start listening for file changes in dir: $uploadDirPath")
    val observer = new FileAlterationObserver(uploadDirPath.toString)
    val monitor = new FileAlterationMonitor(1000)
    val listener = new FileAlterationListenerAdaptor() {

      override def onFileCreate(file: File) = {
        logger.info(s"CREATED: $file")
        addToUploadQueue(file.toPath: Path)
      }

      override def onFileDelete(file: File) = {
        logger.info(s"DELETED: $file")
      }

      override def onFileChange(file: File) = {
        logger.info(s"CHANGED: $file")
      }
    }
    observer.addListener(listener)
    monitor.addObserver(observer)
    monitor.start()
    uploadDirPath
  }

  private def addToUploadQueue(path: Path) = {
    uploadSourceQueue.offer(path).map {
      case QueueOfferResult.Enqueued => logger.info(s"Enqueued: $path")
      case QueueOfferResult.Dropped => logger.info(s"Dropped: $path")
      case QueueOfferResult.Failure(ex) => logger.info(s"Offer failed: $ex")
      case QueueOfferResult.QueueClosed => logger.info("SourceQueue closed")
    }
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

  def countFilesProcessed() = {
    new File(processedDir.toString).list().length
  }

  def stop() = {
    logger.info("About to shutdown DirectoryListener...")
    uploader.stop()
  }
}

object DirectoryListener extends App {
  def apply(uploadDir: Path, processedDir: Path) = new DirectoryListener(uploadDir, processedDir)
}
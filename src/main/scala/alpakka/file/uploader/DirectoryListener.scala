package alpakka.file.uploader

import akka.actor.ActorSystem
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{OverflowStrategy, QueueOfferResult}
import org.apache.commons.io.monitor.{FileAlterationListenerAdaptor, FileAlterationMonitor, FileAlterationObserver}
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.nio.file._
import scala.compat.java8.StreamConverters.StreamHasToScala
import scala.concurrent.Future

/**
  * Pick up (new/changed) files in `rootDir/upload`
  * Do a HTTP file upload via [[Uploader]]
  * Finally move the file to `rootDir/processed`
  *
  * Run with [[DirectoryListenerSpec]]
  *
  * Remarks:
  *  - Using commons.io [[FileAlterationListenerAdaptor]] gives us the ability to recursively listen to file changes
  *  - Currently Alpakka DirectoryChangesSource can not do this, see:
  *    https://discuss.lightbend.com/t/using-directorychangessource-recursively/7630
  */
class DirectoryListener(uploadDir: Path, processedDir:Path ) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("DirectoryListener")
  implicit val executionContext = system.dispatcher

  val uploader = Uploader(system)

  // Handle initial files in structure
  Files
    .walk(uploadDir)
    .filter(path => Files.isDirectory(path))
    .toScala[List]
    .map(path => uploadAllFilesFromSourceDir(path))

  // Handle recursively added files at runtime
  addDirChangeListener(uploadDir)

  val bufferSize = 100
  val maxConcurrentOffers = 1000
  val sourceQueue = Source
    .queue[Path](bufferSize, OverflowStrategy.backpressure, maxConcurrentOffers)
    .mapAsync(1)(path => uploadAndMove(path))
    .to(Sink.ignore)
    .run()


  private def uploadAllFilesFromSourceDir(path: Path) = {
    logger.info(s"About to upload files in dir: $path")

    Directory.ls(path)
      .mapAsync(1)(path => uploadAndMove(path))
      .run()
  }

  private def addDirChangeListener(path: Path) = {
    logger.info(s"About to start listening for dir changes in dir: $path")
    val observer = new FileAlterationObserver(path.toString)
    val monitor = new FileAlterationMonitor(1000)
    val listener = new FileAlterationListenerAdaptor() {

      override def onFileCreate(file: File) {
        logger.info(s"CREATED: $file")

        sourceQueue.offer(file.toPath).map {
          case QueueOfferResult.Enqueued => logger.info(s"enqueued $file")
          case QueueOfferResult.Dropped => logger.info(s"dropped $file")
          case QueueOfferResult.Failure(ex) => logger.info(s"Offer failed: $ex")
          case QueueOfferResult.QueueClosed => logger.info("Source Queue closed")
        }
      }

      override def onFileDelete(file: File) {
        logger.info(s"DELETED: $file")
      }

      override def onFileChange(file: File) {
        logger.info(s"CHANGED: $file")
      }
    }
    observer.addListener(listener)
    monitor.addObserver(observer)
    monitor.start()
    path
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

object DirectoryListener extends App  {
  def apply(uploadDir: Path, processedDir: Path) = new DirectoryListener(uploadDir, processedDir)
}
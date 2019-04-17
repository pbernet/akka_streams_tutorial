package sample.stream_shared_state

import java.io.File
import java.net.URI
import java.nio.file.{Path, Paths}
import java.util.concurrent.ThreadLocalRandom

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorAttributes, ActorMaterializer, Supervision, ThrottleMode}
import org.apache.commons.io.FileUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.control.NonFatal


/**
  * Implement a local file cache with akka-streams statefulMapConcat
  * There was a need to avoid duplicate file downloads and thus keep them locally
  *
  * Use case in detail:
  * Process a stream of messages with reoccurring IDs
  * For the first ID download a .zip file and add it to the local file cache, which has two parts:
  *   In memory in a ListMap, which preserves the insert order
  *   On filesystem in /tmp/localFileCache
  * For each subsequent IDs try to load from local file cache
  *
  * On system restart: read all files from filesystem ordered by lastModified
  * After configurable evictionTime has expired: remove oldest file
  * Simulate download exceptions and resume stream: Supervision.Resume keeps the state
  *
  * TODO
  * Since the http download is executed from the statefulMapConcat stage, the http download can not occur in parallel
  *
  * If used in a ProcessingApp, where faulty elements could occur at a later stage and put in an error queue
  * and thus the original downloaded file needs to remain in file cache for later reference
  * Simulate File.setLastModified to extend the keeping period
  *
  * Multiple logback.xml are on the classpath, read from THIS on in project
  *
  */
object LocalFileCache {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val deciderFlow: Supervision.Decider = {
    case NonFatal(e) =>
      logger.info(s"Stream failed with: $e, going to resume")
      Supervision.Resume
    case _ => Supervision.Stop
  }

  val scaleFactor = 1 //Raise to stress more
  val evictionTime: Long = 10 * 1000 //10 seconds
  val localFileCache = Paths.get(System.getProperty("java.io.tmpdir")).resolve("localFileCache")

  FileUtils.forceMkdir(localFileCache.toFile)
  //Comment out to start with empty local file cache
  //FileUtils.cleanDirectory(localFileCache.toFile)


  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("LocalFileCache")
    implicit val materializer = ActorMaterializer()

    case class Message(id: Int, file: File)

    //Generate random messages with IDs between 0/10
    val messages = List.fill(1000)(Message(ThreadLocalRandom.current.nextInt(0, 10 * scaleFactor), null))


    val stateFlow = Flow[Message].statefulMapConcat { () =>
      //We are in a thread safe env, like in an Actor

      //These internal states are kept by akka streams on Supervision.Resume
      var downloaded = scala.collection.mutable.ListMap.empty[Int, Path]
      var downloadInProgress = scala.collection.mutable.ListBuffer.empty[Int]

      val loadedResults = new FileLister().run(localFileCache.toFile)
      logger.info(s"Loaded ${loadedResults.size} file refs from: $localFileCache...")
      loadedResults.forEach { each: File =>
        logger.debug(s"Add file: ${each.getName} with lastModified: ${each.lastModified()}")
        downloaded += (each.getName.dropRight(4).toInt -> each.toPath)
      }

      logger.info(s"Finished loading: ${loadedResults.size} file refs. Start processing loop...")

      def evictOldestFile() = {
        logger.debug(s"Eviction started on $downloaded")
        if (downloaded.nonEmpty) {
          val timeStampOfOldestFile = downloaded.values.head.toFile.lastModified()
          val oldestFileID = downloaded.keys.head
          logger.debug(s"About to evict oldest cached file with ID: $oldestFileID")
          if (timeStampOfOldestFile + evictionTime < System.currentTimeMillis()) {
            //TODO make more robust, filesystem and memory should not get out of sync
            if (downloaded.nonEmpty){
            downloaded -= oldestFileID
            FileUtils.deleteQuietly(localFileCache.resolve(Paths.get(oldestFileID.toString + ".zip")).toFile)
            logger.info(s"REMOVED oldest file: $oldestFileID after: $evictionTime ms. Remaining files: ${downloaded.keys.size}")
            }
          }
        }
      }

      def processNext(message: Message): List[Message] = {
        if (downloaded.contains(message.id)) {
          logger.info("Cache hit for ID: " + message.id)
          List(message)
        } else {
          logger.info("Cache miss - download for ID: " + message.id)

          if (downloadInProgress.contains(message.id)) {
            logger.info(s"Download is in progress for ID: ${message.id}. Retry after 500 ms. All ID: $downloadInProgress")
            Thread.sleep(500)
            processNext(message)
          }

          var downloadedFile: Path = null
          try {
            downloadInProgress += message.id
            val destinationFile = localFileCache.resolve(Paths.get(message.id.toString + ".zip"))
            val url = new URI("http://127.0.0.1:6001/downloadflaky/" + message.id.toString)
            downloadedFile = new DownloaderRetry().download(url, destinationFile)
            downloaded += (message.id -> downloadedFile)
          } catch {
            case e@(_: Throwable) => {
              logger.error("Ex during download. " + e)

              throw e
            }
          } finally {
            //we need to remove in any case, otherwise we produce a endless loop
            downloadInProgress -= message.id
          }
          List(message)
        }
      }

      message =>
        logger.debug(s"Evicting...")
        evictOldestFile()
        logger.debug(s"Start processing for: ${message.id}")
        processNext(message)

    }

    Source(messages)
      .throttle(10 * scaleFactor, 1.second, 2 * scaleFactor, ThrottleMode.shaping)
      .via(stateFlow)
      //TODO Simulate error in a later processing stage
      .withAttributes(ActorAttributes.supervisionStrategy(deciderFlow))
      .runWith(Sink.seq)
      .onComplete {
        identValues =>
          logger.info(s"Processed: ${identValues.get.size} elements")
          val result: Map[Int, immutable.Seq[Message]] = identValues.get.groupBy(each => each.id)
          result.foreach {
            each: (Int, immutable.Seq[Message]) =>
              logger.info(s"ID: ${each._1} elements: ${each._2.size}")
          }
          system.terminate()
      }
  }
}

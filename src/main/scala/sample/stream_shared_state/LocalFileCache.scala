package sample.stream_shared_state

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.ThreadLocalRandom

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorAttributes, ActorMaterializer, Supervision, ThrottleMode}
import org.apache.commons.io.FileUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable
import scala.collection.immutable.ListMap
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
  * If used in a ProcessingApp, where faulty elements are put in error queue and thus need to remain in file cache
  * Use File.setLastModified to extend the keeping period
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
  val localFileCache = Paths.get("/tmp/localFileCache")

  FileUtils.forceMkdir(localFileCache.toFile)
  //Comment out to start with empty local file cache
  //FileUtils.cleanDirectory(localFileCache.toFile)


  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("LocalFileCache")
    implicit val materializer = ActorMaterializer()

    case class Message(id: Int, file: File)

    //Generate random messages with IDs between 0/10
    val messages = List.fill(100000)(Message(ThreadLocalRandom.current.nextInt(0, 10 * scaleFactor), null))


    val stateFlow = Flow[Message].statefulMapConcat { () =>

      //State with already processed IDs
      var stateMap = ListMap.empty[Int, File]

      logger.info(s"About to load file refs from: $localFileCache...")
      val loadedResults =  new FileLister().run(localFileCache.toFile)
      loadedResults.forEach { each: File =>
        logger.debug(s"Add file: ${each.getName} with lastModified: ${each.lastModified()}")
        stateMap = stateMap + (each.getName.dropRight(4).toInt -> each)
      }

      logger.info(s"Finished loading: ${loadedResults.size} file refs. Start processing loop...")

      def evictOldestFile() = {
        if (stateMap.nonEmpty) {
          val timeStampOfOldestFile = stateMap.values.head.lastModified()
          val oldestFileID = stateMap.keys.head
          logger.debug(s"About to evict oldest cached file with ID: $oldestFileID")
          if (timeStampOfOldestFile + evictionTime < System.currentTimeMillis()) {
            //TODO make more robust, filesystem and memory should not get out of sync
            stateMap = stateMap - oldestFileID
            FileUtils.deleteQuietly(localFileCache.resolve(Paths.get(oldestFileID.toString + ".zip")).toFile)
            logger.info(s"REMOVED oldest file: $oldestFileID after: $evictionTime ms. Remaining files: ${stateMap.keys.size}")
          }
        }
      }

      def processNext(message: Message) = {
        if (stateMap.contains(message.id)) {
          logger.info("Cache hit for ID: " + message.id)
          List(message)
        } else {
          logger.info("Cache miss - download for ID: " + message.id)

          //Simulate download problem, the spec says the the 2nd concurrent request for the same ID gets a HTTP 404
          if (message.id < 2 * scaleFactor) throw new RuntimeException("BOOM")

          //For now: simulate a successfully download file
          val downloadedFile: Path = Files.createFile(Paths.get("/tmp/localFileCache").resolve(Paths.get(message.id.toString + ".zip")))

          stateMap = stateMap + (message.id -> downloadedFile.toFile)
          List(message)
        }
      }

      message =>
        evictOldestFile()
        processNext(message)

    }

    Source(messages)
      .throttle(2 * scaleFactor, 1.second, 2 * scaleFactor, ThrottleMode.shaping)
      .via(stateFlow)
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

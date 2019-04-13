package sample.stream_shared_state

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.ThreadLocalRandom

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.file.scaladsl.Directory
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorAttributes, ActorMaterializer, Supervision, ThrottleMode}
import org.apache.commons.io.FileUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable
import scala.collection.immutable.ListMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
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
  * On system restart: read all files from filesystem
  * After configurable evictionTime has expired: remove oldest file
  * Simulate download exceptions and resume stream: Supervision.Resume keeps the state
  *
  * TODO
  * If used in a ProcessingApp, where faulty elements are put in error queue and thus need to remain in file cache
  * Use File.setLastModified to extend the keeping period
  *
  * After system restart the files are now read in random order, which affects the eviction process.
  * So files remain at-least for the evictionTime in the cache but maybe longer
  *
  * The eviction may be done async
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

  val scaleFactor = 10 //Raise to stress
  val evictionTime: Long = 10 * 1000 //10 seconds
  val localFileCache = Paths.get("/tmp/localFileCache")

  FileUtils.forceMkdir(localFileCache.toFile)
  //Comment out to start with empty cache
  //FileUtils.cleanDirectory(localFileCache.toFile)


  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("LocalFileCache")
    implicit val materializer = ActorMaterializer()

    case class Message(id: Int, file: File)

    //Generate random messages with IDs between 0/10
    val messages = List.fill(100000)(Message(ThreadLocalRandom.current.nextInt(0, 100 * scaleFactor), null))


    val stateFlow = Flow[Message].statefulMapConcat { () =>

      //State with already processed IDs
      var stateMap = ListMap.empty[Int, File]

      println(s"About to load file references from $localFileCache ...")
      //TODO Ordered by last modified TS in Java:
      //https://stackoverflow.com/questions/203030/best-way-to-list-files-in-java-sorted-by-date-modified
      val filesSource: Source[Path, NotUsed] = Directory.ls(localFileCache)

      val result: Future[immutable.Seq[Path]] = filesSource
        //.wireTap(each => println(each))
        .runWith(Sink.seq)

      //Sync loading is needed here
      val loadedResults = Await.result(result, 600.seconds)
      loadedResults.foreach { each: Path =>
        stateMap = stateMap + (each.getFileName.toString.dropRight(4).toInt -> each.toFile)
      }
      println(s"Finished loading: ${loadedResults.size} file references")

      println(s"Start processing loop...")


      def evictOldestFile() = {
        if (stateMap.nonEmpty) {
          val timeStampOfOldestFile = stateMap.values.head.lastModified()
          val oldestFileID = stateMap.keys.head
          //println(s"About to evict oldest cached file with ID: $oldestFileID")
          if (timeStampOfOldestFile + evictionTime < System.currentTimeMillis()) {
            FileUtils.forceDelete(localFileCache.resolve(Paths.get(oldestFileID.toString + ".zip")).toFile)
            stateMap = stateMap - oldestFileID
            println(s"REMOVED oldest file: $oldestFileID after: $evictionTime ms. Remaining files: ${stateMap.keys.size}")
          }
        }
      }

      def processNext(message: Message) = {
        if (stateMap.contains(message.id)) {
          println("Cache hit for ID: " + message.id)
          List(message)
        } else {
          println("Cache miss - download for ID: " + message.id)

          //Simulate download problem
          if (message.id < 2 * scaleFactor) throw new RuntimeException("BOOM")

          //Result of a successful download file
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
          println(s"Processed: ${identValues.get.size} elements")
          val result: Map[Int, immutable.Seq[Message]] = identValues.get.groupBy(each => each.id)
          result.foreach {
            each: (Int, immutable.Seq[Message]) =>
              println(s"ID: ${each._1} elements: ${each._2.size}")
          }
          system.terminate()
      }
  }
}

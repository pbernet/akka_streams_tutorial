package sample.stream_shared_state

import java.io.File
import java.nio.file.{Files, Path, Paths}

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
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.math.abs
import scala.util.Random._
import scala.util.Try
import scala.util.control.NonFatal


/**
  * Implement a local file cache with akka-streams statefulMapConcat
  * There was a need to avoid duplicate file downloads and thus keep them locally
  *
  * Use case in detail:
  * Process a stream of reoccurring IDs
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
  * Originally inspired by:
  * https://stackoverflow.com/questions/37902354/akka-streams-state-in-a-flow
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

  val evictionTime: Long = 10 * 1000 //10 seconds
  val localFileCache = Paths.get("/tmp/localFileCache")

  FileUtils.forceMkdir(localFileCache.toFile)
  //Comment out to start with empty cache
  //FileUtils.cleanDirectory(localFileCache.toFile)


  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("LocalFileCache")
    implicit val materializer = ActorMaterializer()

    case class IdentValue(id: Int, file: File)

    //Generate random IDs between 0/10
    val identValues = List.fill(100)(IdentValue(abs(nextInt()) % 10, null))


    val stateFlow = Flow[IdentValue].statefulMapConcat { () =>

      //State with already processed IDs
      var stateMap = ListMap.empty[Int, File]

      println("About to load file references...")
      val filesSource: Source[Path, NotUsed] = Directory.ls(localFileCache)

      val result: Future[immutable.Seq[Path]] = filesSource
        //.wireTap(each => println(each))
        .runWith(Sink.seq)

      //If sync loading would be needed: Use Await.result(result, xx.seconds)
      result.onComplete {
        results: Try[immutable.Seq[Path]] =>
            results.get.foreach { each: Path =>
              stateMap = stateMap + (each.getFileName.toString.dropRight(4).toInt -> each.toFile)
            }
          println(s"Finished loading file references")
      }


      println(s"Start processing...")


      def evictOldestFile() = {
        if (stateMap.nonEmpty) {
          val timeStampOfOldestFile = stateMap.values.head.lastModified()
          val oldestFileID = stateMap.keys.head
          //println(s"About to evict oldest cached file with ID: $oldestFileID")
          if (timeStampOfOldestFile + evictionTime < System.currentTimeMillis()) {
            FileUtils.forceDelete(localFileCache.resolve(Paths.get(oldestFileID.toString + ".zip")).toFile)
            stateMap = stateMap - oldestFileID
            println(s"REMOVED oldest file: $oldestFileID after: $evictionTime ms. Remaining files: ${stateMap.keys}")
          }
        }
      }

      identValue =>

        evictOldestFile()

        if (stateMap.contains(identValue.id)) {
          println("Cache hit for ID: " + identValue.id)
          List(identValue)
        } else {
          println("Cache miss - download for ID: " + identValue.id)

          //Simulate download problem
          if (identValue.id == 0) throw new RuntimeException("BOOM")

          //Result of a successful download file
          val downloadedFile: Path = Files.createFile(Paths.get("/tmp/localFileCache").resolve(Paths.get(identValue.id.toString + ".zip")))

          stateMap = stateMap + (identValue.id -> downloadedFile.toFile)
          List(identValue)
        }

    }

    Source(identValues)
      .throttle(1, 1.second, 1, ThrottleMode.shaping)
      .via(stateFlow)
      .withAttributes(ActorAttributes.supervisionStrategy(deciderFlow))
      .runWith(Sink.seq)
      .onComplete {
        identValues =>
          println(s"Processed: ${identValues.get.size} elements")
          val result: Map[Int, immutable.Seq[IdentValue]] = identValues.get.groupBy(each => each.id)
          result.foreach {
            each: (Int, immutable.Seq[IdentValue]) =>
              println(s"ID: ${each._1} elements: ${each._2.size}")
          }
          system.terminate()
      }
  }
}

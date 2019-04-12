package sample.stream_shared_state

import java.io.File
import java.nio.file.{Files, Path, Paths}

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorAttributes, ActorMaterializer, Supervision, ThrottleMode}
import org.apache.commons.io.FileUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable
import scala.collection.immutable.ListMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.math.abs
import scala.util.Random._
import scala.util.control.NonFatal


/**
  * Stateful stream processing inspired by:
  * https://stackoverflow.com/questions/37902354/akka-streams-state-in-a-flow
  *
  * Use case:
  * Poor mans local file cache
  * Avoid duplicate downloads eg when each file must be downloaded once only
  *
  * TODO
  * On system restart: read all files from filesystem to ListMap
  *
  */
object StatefulMapConcat {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val deciderFlow: Supervision.Decider = {
    case NonFatal(e) => //FileAlreadyExistsException should not happen after initial load
      logger.info(s"Stream failed with: $e, going to resume (and keep the state)")
      Supervision.Resume
    case _ => Supervision.Stop
  }

  val evictionTime: Long = 2 * 1000 //2 seconds
  val localFileCache = Paths.get("/tmp/localFileCache")

  FileUtils.forceMkdir(Paths.get("/tmp/localFileCache").toFile)
  FileUtils.cleanDirectory(Paths.get("/tmp/localFileCache").toFile)

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("StatefulMapConcat")
    implicit val materializer = ActorMaterializer()

    case class IdentValue(id: Int, file: File)

    //Generate random IDs between 0/10
    val identValues = List.fill(100)(IdentValue(abs(nextInt()) % 10, null))

    val stateFlow = Flow[IdentValue].statefulMapConcat { () =>

      //State with already processed IDs, ListMap preserves insert order
      var stateMap = ListMap.empty[Int, File]

      identValue =>
        if (stateMap.contains(identValue.id)) {
          //Do nothing
          List(identValue)
        } else {
          println("Download for ID: " + identValue.id)

          //Simulate not successful download
          if (identValue.id == 0) throw new RuntimeException("BOOM")

          //Result of a successful download file
          val downloadedFile: Path = Files.createFile(Paths.get("/tmp/localFileCache").resolve(Paths.get(identValue.id.toString + ".zip")))

          stateMap = stateMap + (identValue.id -> downloadedFile.toFile)

          //Evict oldest element after evictionTime
          val timeStampOfOldestFile = stateMap.values.head.lastModified()
          val idOldestFile = stateMap.keys.head
          if (timeStampOfOldestFile + evictionTime < System.currentTimeMillis()) {
            stateMap = stateMap - idOldestFile
            println(s"REMOVED oldest element: $idOldestFile after: $evictionTime ms. Remaining elements in Map: ${stateMap.keys}")

          }
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

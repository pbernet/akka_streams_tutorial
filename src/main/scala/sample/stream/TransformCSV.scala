package sample.stream

import akka.actor.ActorSystem
import org.apache.commons.io.FileUtils
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import scala.language.postfixOps
import scala.sys.process._

/**
  * Inspired by:
  * https://discuss.lightbend.com/t/transform-a-csv-file-into-multiple-csv-files-using-akka-stream/3142
  *
  * Solution proposal in 4 steps according to "use the best tool available" strategy.
  * Thus we use linux tools instead of akka-streams,
  * because we only have "chainsaw style operations" and no business logic.
  *
  * Remarks:
  *  - We could have done all 4 steps in one shell script, but we wanted to show Scala [[Process]] features
  *  - See also: [[FlightDelayStreaming]] where the csv data file originates
  *
  * Doc Scala ProcessBuilder:
  * https://dotty.epfl.ch/api/scala/sys/process/ProcessBuilder.html
  */
object TransformCSV extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  val sourceFile = "src/main/resources/2008_subset.csv"
  val tmpSortedFile = File.createTempFile("sorted_tmp", ".csv")

  val os = System.getProperty("os.name").toLowerCase
  if (os == "mac os x") {
    val pwd = "pwd".!!
    logger.info(s"Running with base path: $pwd")

    // 1) Remove csv header and use linux sort on the 9th column (= UniqueCarrier)
    val resultSort = (Process(s"tail -n+2 $sourceFile") #| Process(s"sort -t\",\" -k9,9") #>> tmpSortedFile).!
    logger.info(s"Exit code sort: $resultSort")

    // 2) Split into files according to value of 9th column
    "rm -rf results".!
    "mkdir -p results".!
    val bashLine = """awk -F ',' '{print > ("results/"$9"-xx.csv")}' """ + s"$tmpSortedFile"
    val resultSplit = Seq("bash", "-c", bashLine).!
    logger.info(s"Exit code split: $resultSplit")

    // 3) Get line count report
    val countLine = s"""wc -l `find results -type f`"""
    val resultCountLine = Seq("bash", "-c", countLine).!!
    logger.info(s"Line count report: $resultCountLine")
    val reportCleaned = resultCountLine.split("\\s+").tail.reverse.tail.tail

    // 4) Add line count to result filename
    reportCleaned.sliding(2, 2).foreach { each =>
      val (path, count) = (each.head, each.last)
      logger.info(s"About to rename file: $path , with count: $count")
      FileUtils.moveFile(FileUtils.getFile(path), FileUtils.getFile(path.replace("xx", count)))
    }

    println("Success. About to terminate...")
    system.terminate()
  }
}
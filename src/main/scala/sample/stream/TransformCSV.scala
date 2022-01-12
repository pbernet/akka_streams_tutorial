package sample.stream

import akka.actor.ActorSystem
import org.apache.commons.io.FileUtils
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.util.concurrent.TimeUnit
import scala.language.postfixOps
import scala.sys.process._

/**
  * Inspired by:
  * https://discuss.lightbend.com/t/transform-a-csv-file-into-multiple-csv-files-using-akka-stream/3142
  *
  * Solution proposal in three steps according to "use the best tool available" strategy.
  * Thus we use linux tools instead of akka-streams
  * because we only have "chainsaw style operations" and no business logic.
  *
  * Remarks:
  *  - Instead of putting all steps in a shell script, we want to use Scala [[Process]] for each step
  *  - Yes, the steps have side effects
  *  - See also: [[FlightDelayStreaming]] where the csv data file originates
  *
  * Doc Scala ProcessBuilder:
  * https://dotty.epfl.ch/api/scala/sys/process/ProcessBuilder.html
  */
object TransformCSV extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  val sourceFile = "src/main/resources/2008_subset.csv"

  val os = System.getProperty("os.name").toLowerCase
  if (os == "mac os x") {
    val tmpSortedFile = sort(sourceFile)
    val resultsDir = split(tmpSortedFile)
    countLines(resultsDir)
  } else {
    logger.warn("OS not supported")
  }
  system.terminate()


  def sort(sourceFile: String) = {
    val pwd = "pwd".!!
    logger.info(s"Running with base path: $pwd")
    val tmpSortedFile = File.createTempFile("sorted_tmp", ".csv")

    // Remove csv header and use linux sort on the 9th column (= UniqueCarrier)
    val resultSort = exec("sort") { (Process(s"tail -n+2 $sourceFile") #| Process(s"sort -t\",\" -k9,9") #>> tmpSortedFile).!}
    logger.info(s"Exit code sort: $resultSort")
    tmpSortedFile
  }

  def split(tmpSortedFile: File)  = {
    "rm -rf results".!
    "mkdir -p results".!

    // Split into files according to value of 9th column (incl. file closing)
    // TODO Make dynamic, but still readable ($ is used in both contexts...)
    val bashLine = """awk -F ',' '{out=("results/"$9"-xx.csv")} out!=prev {close(prev)} {print > out; prev=out}' """ + s"$tmpSortedFile"
    val resultSplit = exec("split") { Seq("bash", "-c", bashLine).!}
    logger.info(s"Exit code split: $resultSplit")
    "results"
  }

  def countLines(results: String) = {
    val bashLine = s"""wc -l `find $results -type f`"""
    val resultCountLines = exec("count") {Seq("bash", "-c", bashLine).!!}
    logger.info(s"Line count report:\n $resultCountLines")

    val reportCleaned = resultCountLines.split("\\s+").tail.reverse.tail.tail
    reportCleaned.sliding(2, 2).foreach { each =>
      val (path, count) = (each.head, each.last)
      logger.info(s"About to rename file: $path , with count: $count")
      FileUtils.moveFile(FileUtils.getFile(path), FileUtils.getFile(path.replace("xx", count)))
    }
  }

  private def exec[R](op: String = "")(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    val elapsedTimeMs = TimeUnit.MILLISECONDS.convert(t1 - t0, TimeUnit.NANOSECONDS)
    logger.info(s"Elapsed time to '$op': $elapsedTimeMs ms")
    result
  }
}
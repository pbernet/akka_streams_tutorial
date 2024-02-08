package sample.stream

import org.apache.commons.io.FileUtils
import org.apache.pekko.actor.ActorSystem
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.util.concurrent.TimeUnit
import scala.language.postfixOps
import scala.sys.process._

/**
  * Inspired by:
  * https://discuss.lightbend.com/t/transform-a-csv-file-into-multiple-csv-files-using-akka-stream/3142
  *
  * Instead of pekko-streams we use linux tools (sort/split) for each step to apply the
  * "chainsaw style" transformations to the large csv file.
  * With the additional count this does the same as: [[FlightDelayStreaming]].
  * However the performance is not as good as in FlightDelayStreaming.
  *
  * Remarks:
  *  - Instead of putting all steps in a shell script, we want to use Scala [[Process]] for each step
  *  - Doc Scala ProcessBuilder: https://dotty.epfl.ch/api/scala/sys/process/ProcessBuilder.html
  */
object TransformCSV extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  val sourceFile = "src/main/resources/2008_subset.csv"
  val resultsDir = "results"
  val countPlaceholder = "_count_"

  val os = System.getProperty("os.name").toLowerCase
  if (os == "mac os x") {
    split(sort(sourceFile))
    countLines(resultsDir)
  } else {
    logger.warn("OS not supported")
  }
  system.terminate()


  def sort(sourceFile: String): File = {
    val op = "sort"
    val tmpSortedFile = File.createTempFile("sorted_tmp", ".csv")

    // Remove csv header and use linux sort on the 9th column (= UniqueCarrier)
    val resultSort = exec(op) {
      (Process(s"tail -n+2 $sourceFile") #| Process(s"sort -t\",\" -k9,9") #>> tmpSortedFile).!
    }
    logger.info(s"Exit code '$op': $resultSort")
    tmpSortedFile
  }

  def split(tmpSortedFile: File): Unit = {
    val op = "split"
    s"rm -rf $resultsDir".!
    s"mkdir -p $resultsDir".!

    // Split into files according to value of 9th column (incl. file closing)
    // Note that the $ operator is used in the linux cmd
    val bashCmdSplit = s"""awk -F ',' '{out=("$resultsDir""" + """/"$9"-_count_.csv")} out!=prev {close(prev)} {print > out; prev=out}' """ + s"$tmpSortedFile"
    val resultSplit = exec(op) {
      Seq("bash", "-c", bashCmdSplit).!
    }
    logger.info(s"Exit code '$op': $resultSplit")
  }

  def countLines(results: String): Unit = {
    val op = "count"
    val bashCmdCountLines = s"""wc -l `find $results -type f`"""
    val resultCountLines = exec(op) {
      Seq("bash", "-c", bashCmdCountLines).!!
    }
    logger.info(s"Line count report:\n $resultCountLines")

    val reportCleaned = resultCountLines.split("\\s+").tail.reverse.tail.tail
    reportCleaned.sliding(2, 2).foreach { each =>
      val (path, count) = (each.head, each.last)
      logger.info(s"About to rename file: $path with count value: $count")
      FileUtils.moveFile(FileUtils.getFile(path), FileUtils.getFile(path.replace(countPlaceholder, count)))
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
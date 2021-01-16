package util


import java.nio.file.{FileSystem, FileSystems, Path}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.file.scaladsl.FileTailSource
import akka.stream.scaladsl.{Keep, Sink, Source}

import scala.concurrent.Await
import scala.concurrent.duration._


class LogFileScanner {
  implicit val system = ActorSystem("LogFileScanner")
  implicit val executionContext = system.dispatcher

  private val fs: FileSystem = FileSystems.getDefault

  // The patterns are case sensitive
  def run(scanDelaySeconds: Int = 0, searchAfterPattern: String, pattern: String): List[String] = {
    val localLogFilePath: String = "./logs/application.log"
    val path: Path = fs.getPath(localLogFilePath)
    val pollingInterval = 250.millis
    val maxLineSize: Int = 24 * 1024

    // Wait for the components to produce log messages
    Thread.sleep(scanDelaySeconds * 1000)
    println(String.format("About to start LogFileScanner - Searching file: %s for pattern: '%s', consider only lines after: '%s'", path, pattern, searchAfterPattern))
    val lines: Source[String, NotUsed] = FileTailSource.lines(path, maxLineSize, pollingInterval)

    val (killSwitch, resultFut) =
      lines
        .dropWhile(line => !line.contains(searchAfterPattern))
        .filter(line => line.contains(pattern))
        //.wireTap(line => println("Found: " + line))
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

    Thread.sleep(1000)
    killSwitch.shutdown()

    val resultList = Await.result(resultFut, 2.seconds)
    println(s"Occurrences found: ${resultList.length}")
    resultList.toList
  }
}

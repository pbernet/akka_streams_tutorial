package util


import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.file.scaladsl.FileTailSource
import akka.stream.scaladsl.{Keep, Sink, Source}

import java.nio.file.{FileSystem, FileSystems, Path, Paths}
import java.util.Scanner
import scala.concurrent.Await
import scala.concurrent.duration._

class LogFileScanner(localLogFilePath: String = "logs/application.log") {
  implicit val system: ActorSystem = ActorSystem()

  private val fs: FileSystem = FileSystems.getDefault

  // The patterns are case sensitive
  def run(scanDelaySeconds: Int = 0, scanForSeconds: Int = 5, searchAfterPattern: String, pattern: String): List[String] = {
    val path: Path = fs.getPath(localLogFilePath)
    val pollingInterval = 250.millis
    val maxLineSize: Int = 24 * 1024

    // Wait for the components to produce log messages
    Thread.sleep(scanDelaySeconds * 1000)
    println(String.format("About to start LogFileScanner - Searching file: %s for pattern: '%s', consider only lines after: '%s'", path, pattern, searchAfterPattern))

    val lineSeparator = detectLineSeparator(Paths.get(localLogFilePath))
    val lines: Source[String, NotUsed] = FileTailSource.lines(path, maxLineSize, pollingInterval, lineSeparator)

    val (killSwitch, resultFut) =
      lines
        .dropWhile(line => !line.contains(searchAfterPattern))
        //.wireTap(line => println("Process line: " + line))
        .filter(line => line.contains(pattern))
        //.wireTap(line => println("Found pattern in line: " + line))
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

    // Time to scan in 'tail -f' mode
    Thread.sleep(scanForSeconds * 1000)
    killSwitch.shutdown()

    val resultList = Await.result(resultFut, 1.seconds)
    println(s"Occurrences found: ${resultList.length}")
    resultList.toList
  }

  /**
    * To process files that were created on different operating systems.
    *
    * @param path
    * @return the line separator for the file
    */
  def detectLineSeparator(path: Path): String = {

    // Assumption: Small buffer (1024 chars) is sufficient to read 1st line
    val scanner = new Scanner(path)
    try {
      scanner.useDelimiter("\\z")
      val firstLine = scanner.next()

      if (firstLine.matches("(?s).*(\\r\\n).*")) {
        println(s"Detected line.separator for: Windows")
        "\r\n"
      }
      else if (firstLine.matches("(?s).*(\\n).*")) {
        println(s"Detected line.separator for: Unix/Linux")
        "\n"
      }
      else if (firstLine.matches("(?s).*(\\r).*")) {
        println(s"Detected line.separator for: Legacy mac os 9")
        "\r"
      }
      else {
        println(s"Unable to detected line.separator, fallback to separator for: ${System.getProperty("os.name")}")
        System.getProperty("line.separator")
      }
    } finally {
      scanner.close()
    }
  }
}

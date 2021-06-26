package alpakka.file

import akka.actor.ActorSystem
import akka.stream.alpakka.file.scaladsl.LogRotatorSink
import akka.stream.scaladsl.{FileIO, Flow, Keep, Source}
import akka.util.ByteString

import java.io.File
import java.nio.file.Path
import java.nio.file.StandardOpenOption._

/**
  * Inspired by:
  * https://discuss.lightbend.com/t/writing-element-each-to-its-own-file-problem-on-last-element/7696
  *
  * The issue mentioned is fixed now with:
  * https://github.com/akka/alpakka/pull/2559
  *
  * So all .txt files are written with the correct content
  * 
  */
object FileRotator extends App {
  implicit val system = ActorSystem("FileRotator")
  implicit val executionContext = system.dispatcher

  val logRotatorSink = {
    LogRotatorSink.withSinkFactory(
      triggerGeneratorCreator =
        () => n => Some(new File(new String(s"file${n.decodeString("UTF-8")}" + ".txt")).toPath),
      sinkFactory =
        (path: Path) =>
          Flow[ByteString].toMat(FileIO.toPath(path, Set(CREATE, WRITE, TRUNCATE_EXISTING, SYNC)))(Keep.right)
    )
  }

  val done =
    Source(1 to 4)
      .map(i => ByteString.fromString(i.toString))
      .runWith(logRotatorSink)

  done.onComplete { _ => system.terminate()}
}

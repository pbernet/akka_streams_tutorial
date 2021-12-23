package alpakka.file

import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.scaladsl.FileIO

import java.nio.file.Paths
import scala.concurrent.Future
import scala.util.{Failure, Success}

/** FileIO echo flow with base64 encoding/decoding:
  *
  * testfile.jpg -> base64 encoding -> testfile.enc -> base64 decoding -> testfile_result.jpg
  *
  * Remark:
  *
  * The chunkSize of the encoding file source MUST be a multiples of 3 byte, eg 3000
  *
  * @see [[https://stackoverflow.com/questions/7920780/is-it-possible-to-base64-encode-a-file-in-chunks]]
  *
  */
object FileIOEcho extends App {
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val sourceFileName = "src/main/resources/testfile.jpg"
  val encFileName = "testfile.enc"
  val resultFileName = "testfile_result.jpg"

  val sourceOrig = FileIO.fromPath(Paths.get(sourceFileName), 3000)
  val sinkEnc = FileIO.toPath(Paths.get(encFileName))

  val doneEnc = sourceOrig
    //.wireTap(each => println(s"Chunk enc: $each"))
    .map(each => each.encodeBase64)
    .runWith(sinkEnc)

  doneEnc.onComplete {
    case Success(_) =>
      val sourceEnc = FileIO.fromPath(Paths.get(encFileName))
      val sinkDec = FileIO.toPath(Paths.get(resultFileName))

      val doneDec = sourceEnc
        //.wireTap(each => println(s"Chunk dec: $each"))
        .map(each => each.decodeBase64)
        .runWith(sinkDec)
      terminateWhen(doneDec)
    case Failure(ex) => println(s"Exception: $ex")
  }

  def terminateWhen(done: Future[IOResult]) = {
    done.onComplete {
      case Success(_) =>
        println(s"Flow Success. Written file: $resultFileName About to terminate...")
        system.terminate()
      case Failure(e) =>
        println(s"Flow Failure: $e. About to terminate...")
        system.terminate()
    }
  }
}
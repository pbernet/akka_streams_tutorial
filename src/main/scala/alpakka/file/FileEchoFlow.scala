package alpakka.file

import java.nio.file.Paths
import java.util.Base64

import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.scaladsl.FileIO
import akka.util.ByteString

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Alpakka File echo flow with sth to process:
  * file -> base64 encoding -> file -> base64 decoding -> file
  *
  * Remark:
  * The chunkSize of the encoding file source MUST be a multiples of 3 byte, eg 3000
  * see: https://stackoverflow.com/questions/7920780/is-it-possible-to-base64-encode-a-file-in-chunks
  *
  */
object FileEchoFlow extends App {
  implicit val system = ActorSystem("FileEchoFlow")
  implicit val executionContext = system.dispatcher

  val enc1 = Base64.getEncoder
  val enc2 = Base64.getMimeEncoder
  val enc3 = Base64.getUrlEncoder

  val dec1 = Base64.getDecoder
  val dec2 = Base64.getMimeDecoder
  val dec3 = Base64.getUrlDecoder

  val sourceOrig = FileIO.fromPath(Paths.get("./src/main/resources/testfile.jpg"), 3000)
  val encFileName = "test.enc"
  val sinkEnc = FileIO.toPath(Paths.get(encFileName))

  val doneEnc = sourceOrig
    .wireTap(each => println(s"Chunk enc: $each"))
    .map(each => ByteString(enc1.encode(each.toByteBuffer)))
    .runWith(sinkEnc)

  doneEnc.onComplete {
    case Success(_) => {
      val sourceEnc = FileIO.fromPath(Paths.get(encFileName))
      val sinkDec = FileIO.toPath(Paths.get("test_result.jpg"))
      val doneDec = sourceEnc
        .wireTap(each => println(s"Chunk dec: $each"))
        .map(each => ByteString(dec1.decode(each.toByteBuffer)))
        .runWith(sinkDec)
      terminateWhen(doneDec)
    }
    case Failure(ex) => println(s"Exception: $ex")
  }

  def terminateWhen(done: Future[IOResult]) = {
    done.onComplete {
      case Success(b) =>
        println("Flow Success. About to terminate...")
        system.terminate()
      case Failure(e) =>
        println(s"Flow Failure: $e. About to terminate...")
        system.terminate()
    }
  }
}
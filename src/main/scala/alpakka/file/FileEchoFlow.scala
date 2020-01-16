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
  * file -> base64 encoding -> file -> base64 decoding -> file
  *
  * TODO Compiles, but:
  * FlowFailure on decoding, BUT probably encoding does not work correctly
  * Decoding on its own works testfile.jpg1.enc
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

  val sourceOrig = FileIO.fromPath(Paths.get("./src/main/resources/testfile.jpg"), 1000)
  val encFileName = "test.enc"
  val sinkEnc = FileIO.toPath(Paths.get(encFileName))

  val doneEnc = sourceOrig
    .wireTap(each => println(s"Chunk enc: $each"))
    //Does not seem to work, because the result can not be used
    .map(each => ByteString(enc1.encode(each.toByteBuffer)))
    //.map(each => ByteString(enc1.encode(StringUtils.strip(each.utf8String, "\n\t").getBytes("UTF-8"))))
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
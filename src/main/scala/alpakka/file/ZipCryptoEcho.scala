package alpakka.file

import java.nio.file.Paths
import java.security._

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.file.ArchiveMetadata
import akka.stream.alpakka.file.scaladsl.Archive
import akka.stream.scaladsl._
import akka.stream.stage._
import akka.util.ByteString
import javax.crypto._
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * File echo flow with Zip archive/un-archive and AES 256 encryption/decryption:
  * 
  * 63MB.pdf (2) -> archive (Archive.zip()) ->
  * AES 128 encryption -> testfile.encrypted -> AES 128 decryption -> testfile_decrypted.zip ->
  * un-archive (ArchiveHelper.unzip) -> echo_(1/2)63MB.pdf
  *
  * Make sure to run with a recent openjdk or with graalvm
  *
  * Inspired by:
  * https://doc.akka.io/docs/alpakka/current/file.html#zip-archive
  * https://gist.github.com/TimothyKlim/ec5889aa23400529fd5e
  *
  */

private[this] class AesStage(cipher: Cipher) extends GraphStage[FlowShape[ByteString, ByteString]] {
  val in = Inlet[ByteString]("in")
  val out = Outlet[ByteString]("out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val bs = grab(in)
          if (bs.isEmpty) push(out, bs)
          else push(out, ByteString(cipher.update(bs.toArray)))
        }

        override def onUpstreamFinish(): Unit = {
          val bs = ByteString(cipher.doFinal())
          if (bs.nonEmpty) emit(out, bs)
          complete(out)
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })
    }
}

object ZipCryptoEcho extends App {
  implicit val system = ActorSystem("ZipCryptoEcho")
  implicit val executionContext = system.dispatcher
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val aesKeySize = 256
  val aesKey = generateAesKey()
  //For simplicity we do not transport, the IV as part of the file
  val initialisationVector = generateIv()

  val sourceFileName = "63MB.pdf"
  val sourceFilePath = s"src/main/resources/$sourceFileName"
  val encFileName = "testfile.encrypted"
  val decFileName = "testfile_decrypted.zip"

  val fileStream1 = FileIO.fromPath(Paths.get(sourceFilePath))
  val fileStream2 = FileIO.fromPath(Paths.get(sourceFilePath))

  val filesStream = Source(
    List(
      (ArchiveMetadata(s"1_$sourceFileName"), fileStream1),
      (ArchiveMetadata(s"2_$sourceFileName"), fileStream2)
    )
  )

  val sinkEnc = FileIO.toPath(Paths.get(encFileName))
  val sinkDec = FileIO.toPath(Paths.get(decFileName))

  logger.info("Start archiving...")
  val sourceZipped = filesStream.via(Archive.zip())

  logger.info("Start encryption...")
  val sourceEnc = encryptAes(sourceZipped, aesKey, initialisationVector)
  val doneEnc = sourceEnc.runWith(sinkEnc)


  doneEnc.onComplete {
    case Success(_) =>
      logger.info("Start decryption...")
      val doneDec = decryptAes(sourceEnc, aesKey, initialisationVector).runWith(sinkDec)
      doneDec.onComplete {
        case Success(_) =>
          // Because we don't have support for un-archive in alpakka files, we use the ArchiveHelper
          logger.info("Start un-archiving...")
          val resultFileContentFut =
            FileIO.fromPath(Paths.get(decFileName)).runWith(Sink.fold(ByteString.empty)(_ ++ _))
          val resultFileContent = Await.result(resultFileContentFut, 10.seconds)
          val unzipResultMap = new ArchiveHelper().unzip(resultFileContent).asScala
          unzipResultMap.foreach(each => {
            logger.info(s"Unzipped file: ${each._1}")
            Source
              .single(each._2)
              .runWith(FileIO.toPath(Paths.get(s"echo_${each._1}")))
          })
          logger.info(s"Saved: ${unzipResultMap.size} echo files")
          system.terminate()
        case Failure(ex) => logger.info(s"Exception: $ex"); system.terminate()
      }
    case Failure(ex) => logger.info(s"Exception: $ex"); system.terminate()
  }

  def generateAesKey() = {
    val gen = KeyGenerator.getInstance("AES")
    gen.init(aesKeySize)
    val key = gen.generateKey()
    val aesKey = key.getEncoded
    aesKeySpec(aesKey)
  }

  def aesKeySpec(key: Array[Byte]) =
    new SecretKeySpec(key, "AES")

  def generateIv() = new SecureRandom().generateSeed(16)

  private def aesCipher(mode: Int, keySpec: SecretKeySpec, ivBytes: Array[Byte]) = {
    val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
    val ivSpec = new IvParameterSpec(ivBytes)
    cipher.init(mode, keySpec, ivSpec)
    cipher
  }

  def encryptAes(
                  source: Source[ByteString, Any],
                  keySpec: SecretKeySpec,
                  ivBytes: Array[Byte]
                ): Source[ByteString, Any] = {
    val cipher = aesCipher(Cipher.ENCRYPT_MODE, keySpec, ivBytes)
    source.via(new AesStage(cipher))
  }

  def decryptAes(
                  source: Source[ByteString, Any],
                  keySpec: SecretKeySpec,
                  ivBytes: Array[Byte]
                ): Source[ByteString, Any] = {
    val cipher = aesCipher(Cipher.DECRYPT_MODE, keySpec, ivBytes)
    source.via(new AesStage(cipher))
  }
}

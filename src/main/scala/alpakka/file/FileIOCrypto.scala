package alpakka.file

import java.nio.file.Paths
import java.security._
import java.security.spec.{PKCS8EncodedKeySpec, X509EncodedKeySpec}

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._
import akka.util.ByteString
import javax.crypto._
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * FileIO echo flow with AES 256 encryption/decryption to give the CPU sth to do:
  * testfile.jpg -> AES 256 encryption -> testfile.encrypted -> AES 256 decryption -> testfile_result.jpg
  *
  * Inspired by:
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
            val bs = ByteString(cipher.doFinal)
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

  object FileIOCrypto extends App {
    implicit val system = ActorSystem("FileIOCrypto")
    implicit val executionContext = system.dispatcher

    val aesKeySize = 256
    val aesKey = generateAesKey()
    val rand = new SecureRandom()
    val initialisationVector = generateIv()

    val sourceFileName = "./src/main/resources/testfile.jpg"
    val encFileName = "testfile.encrypted"
    val resultFileName = "testfile_result.jpg"

    val sourceOrig = FileIO.fromPath(Paths.get(sourceFileName))
    val sinkEnc = FileIO.toPath(Paths.get(encFileName))
    val sinkDec = FileIO.toPath(Paths.get(resultFileName))

    val source: Source[ByteString, Any] = encryptAes(sourceOrig, aesKey, initialisationVector)
    val doneEnc = source
        //.wireTap(each => println(each))
        .runWith(sinkEnc)

    doneEnc.onComplete {
      case Success(_) =>
        val sourceEnc = FileIO.fromPath(Paths.get(encFileName))
        val sinkDec = FileIO.toPath(Paths.get(resultFileName))

        val doneDec = decryptAes(sourceEnc, aesKey, initialisationVector)
          //.wireTap(each => println(each))
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

    def generateAesKey() = {
      val gen = KeyGenerator.getInstance("AES")
      gen.init(aesKeySize)
      val key = gen.generateKey()
      val aesKey = key.getEncoded
      aesKeySpec(aesKey)
    }

    def aesKeySpec(key: Array[Byte]) =
      new SecretKeySpec(key, "AES")

    def generateIv() = rand.generateSeed(16)

    private def aesCipher(mode: Int, keySpec: SecretKeySpec, ivBytes: Array[Byte]) = {
      val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
      val ivSpec = new IvParameterSpec(ivBytes)
      cipher.init(mode, keySpec, ivSpec)
      cipher
    }

    def encryptAes(
                    source:  Source[ByteString, Any],
                    keySpec: SecretKeySpec,
                    ivBytes: Array[Byte]
                  ): Source[ByteString, Any] = {
      val cipher = aesCipher(Cipher.ENCRYPT_MODE, keySpec, ivBytes)
      source.via(new AesStage(cipher))
    }

    def decryptAes(
                    source:  Source[ByteString, Any],
                    keySpec: SecretKeySpec,
                    ivBytes: Array[Byte]
                  ): Source[ByteString, Any] = {
      val cipher = aesCipher(Cipher.DECRYPT_MODE, keySpec, ivBytes)
      source.via(new AesStage(cipher))
    }

    def getRsaKeyFactory() =
      KeyFactory.getInstance("RSA")

    def loadRsaPrivateKey(key: Array[Byte]) = {
      val spec = new PKCS8EncodedKeySpec(key)
      getRsaKeyFactory().generatePrivate(spec)
    }

    def loadRsaPublicKey(key: Array[Byte]) = {
      val spec = new X509EncodedKeySpec(key)
      getRsaKeyFactory().generatePublic(spec)
    }

    private def rsaCipher(mode: Int, key: Key) = {
      val cipher = Cipher.getInstance("RSA")
      cipher.init(mode, key)
      cipher
    }

    def encryptRsa(bytes: Array[Byte], key: PublicKey): Array[Byte] =
      rsaCipher(Cipher.ENCRYPT_MODE, key).doFinal(bytes)

    def decryptRsa(bytes: Array[Byte], key: PrivateKey): Array[Byte] =
      rsaCipher(Cipher.DECRYPT_MODE, key).doFinal(bytes)
  }

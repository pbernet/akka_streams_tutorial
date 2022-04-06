package alpakka.file

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.file.ArchiveMetadata
import akka.stream.alpakka.file.scaladsl.Archive
import akka.stream.scaladsl._
import akka.stream.stage._
import akka.util.ByteString
import org.slf4j.{Logger, LoggerFactory}

import java.io.FileInputStream
import java.nio.file.Paths
import java.security._
import javax.crypto._
import javax.crypto.spec.{GCMParameterSpec, IvParameterSpec, SecretKeySpec}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

/** File echo flow with Zip archive/un-archive and AES 256 CBC/GCM encryption/decryption:
  *
  * 63MB.pdf (2) -> archive (Archive.zip()) ->
  * AES encryption -> testfile.encrypted -> AES decryption -> testfile_decrypted.zip ->
  * un-archive (ArchiveHelper.unzip) -> echo_(1/2)_63MB.pdf
  *
  * Remarks:
  *  - For AES/CBC: initialisationVector is at the first 16 Bytes of the encrypted file
  *  - For AES/GCM: initialisationVector is at the first 12 Bytes of the encrypted file
  *  - Run with a recent Java 11 openjdk or with graalvm to get the 256 Bit key size
  *  - For cipher "ChaCha20-Poly1305/None/NoPadding" comment in the code bits below
  *
  * A word of caution of this concept PoC regarding AES 256 GCM encryption/decryption:
  *
  * We are using the provided Sun JCE ciphers here. Since the decryption performance of
  * AES 256 GCM on large files is really poor, you should consider the cipher from
  * [[https://bouncycastle.org]] , which is faster and behaves in a "linear fashion".
  *
  * Inspired by:
  *  - [[https://doc.akka.io/docs/alpakka/current/file.html#zip-archive]]
  *  - [[https://gist.github.com/TimothyKlim/ec5889aa23400529fd5e]]
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
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val aesKeySize = 256
  val aesKey = generateAesKey()
  val aesMode = "AES/CBC" // Switch to "AES/GCM" or "ChaCha20"
  val ivLengthBytes = if (aesMode == "AES/CBC") 16 else 12
  val initialisationVector = generateNonce(ivLengthBytes)

  // Activate for ChaCha20-Poly1305/None/NoPadding
  // Uses built in Java 11 cipher: https://openjdk.java.net/jeps/329
  // To run at least openjdk 11.0.8_10 is required
  // This Issue is fixed there: https://github.com/eclipse/openj9/issues/9535
  //val chaCha20Nonce = generateNonce(ivLengthBytes)
  //val chaCha20Key = generateChaCha20Key()

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

  logger.info(s"Start encryption zip file with: $aesMode...")
  val sourceEnc = encryptAes(sourceZipped, aesKey, initialisationVector, aesMode)
  //val sourceEnc = encryptChaCha20(sourceZipped, chaCha20Key)

  // Prepend IV
  val ivSource = Source.single(ByteString(initialisationVector))

  val doneEnc = sourceEnc
    // Comment out "merge" for ChaCha20-Poly1305/None/NoPadding
    .merge(ivSource)
    .runWith(sinkEnc)


  doneEnc.onComplete {
    case Success(_) =>
      logger.info(s"Start decryption zip file with: $aesMode...")


      val doneDec = decryptAesFromFile(encFileName, aesKey, aesMode).runWith(sinkDec)
      //val doneDec = decryptChaCha20FromFile(encFileName, chaCha20Key).runWith(sinkDec)

      doneDec.onComplete {
        case Success(_) =>
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

  private def aesCipherCBC(mode: Int, keySpec: SecretKeySpec, ivBytes: Array[Byte]) = {
    val cipher = Cipher.getInstance("AES/CBC/PKCS5Padding")
    val ivSpec = new IvParameterSpec(ivBytes)
    cipher.init(mode, keySpec, ivSpec)
    cipher
  }

  private def aesCipherGCM(mode: Int, keySpec: SecretKeySpec, ivBytes: Array[Byte]) = {
    val cipher = Cipher.getInstance("AES/GCM/NoPadding")

    // For GCM we need additional "Authentication Tag length in bits"
    val gcmParameterSpec = new GCMParameterSpec(16 * 8, ivBytes)
    cipher.init(mode, keySpec, gcmParameterSpec)
    cipher
  }

  def encryptAes(
                  source: Source[ByteString, Any],
                  keySpec: SecretKeySpec,
                  ivBytes: Array[Byte],
                  aesMode: String
                ): Source[ByteString, Any] = {
    val cipher = aesMode match {
      case "AES/GCM" => aesCipherGCM(Cipher.ENCRYPT_MODE, keySpec, ivBytes)
      case _ => aesCipherCBC(Cipher.ENCRYPT_MODE, keySpec, ivBytes)
    }
    source.via(new AesStage(cipher))
  }

  def decryptAes(
                  source: Source[ByteString, Any],
                  keySpec: SecretKeySpec,
                  ivBytes: Array[Byte],
                  aesMode: String
                ): Source[ByteString, Any] = {

    val cipher = aesMode match {
      case "AES/GCM" => aesCipherGCM(Cipher.DECRYPT_MODE, keySpec, ivBytes)
      case _ => aesCipherCBC(Cipher.DECRYPT_MODE, keySpec, ivBytes)
    }
    source.via(new AesStage(cipher))
  }

  def decryptAesFromFile(
                          fileName: String,
                          keySpec: SecretKeySpec,
                          aesMode: String
                        ): Source[ByteString, Any] = {

    // Read IV (first n bytes from stream), good old Java to the rescue
    // Surprisingly difficult in akka streams
    // https://stackoverflow.com/questions/61822306/reading-first-bytes-from-akka-stream-scaladsl-source
    // https://stackoverflow.com/questions/40743047/handle-akka-streams-first-element-specially

    val ivBytesBuffer = new Array[Byte](ivLengthBytes)
    val is = new FileInputStream(fileName)
    is.read(ivBytesBuffer)

    // We need a large chunk size here to speed up the AES/GCM decryption
    // This is an issue in the Java world, because CipherInputStream has a limited buffer size of 512 bytes
    // https://stackoverflow.com/questions/60575897/cipherinputstream-hangs-while-reading-data
    // However, even if we stream all the data, it looks as if everything is loaded into memory
    val source = StreamConverters.fromInputStream(() => is, chunkSize = 10000 * 1024)
    decryptAes(source, keySpec, ivBytesBuffer, aesMode)
  }

  private def generateNonce(numBytes: Integer) = {
    new SecureRandom().generateSeed(numBytes)
  }

  //Activate for ChaCha20-Poly1305/None/NoPadding

  //  def encryptChaCha20(
  //                       source: Source[ByteString, Any],
  //                       keySpec: SecretKeySpec
  //                     ): Source[ByteString, Any] = {
  //    val ivSpec = new IvParameterSpec(chaCha20Nonce)
  //    val cipher = chaCha20Cipher(Cipher.ENCRYPT_MODE, keySpec, ivSpec)
  //    source.via(new AesStage(cipher))
  //  }
  //
  //  def decryptChaCha20FromFile(
  //                       fileName: String,
  //                       keySpec: SecretKeySpec
  //                     ): Source[ByteString, Any] = {
  //    //Must go via file, otherwise the AesStage can not initialize the cipher correctly
  //    val is = new FileInputStream(fileName)
  //    val source = StreamConverters.fromInputStream(() => is)
  //
  //    val ivSpec = new IvParameterSpec(chaCha20Nonce)
  //    val cipher = chaCha20Cipher(Cipher.DECRYPT_MODE, keySpec, ivSpec)
  //    source.via(new AesStage(cipher))
  //  }
  //
  //  private def chaCha20Cipher(mode: Int, keySpec: SecretKeySpec, ivSpec: IvParameterSpec) = {
  //    val cipher = Cipher.getInstance("ChaCha20-Poly1305/None/NoPadding")
  //    cipher.init(mode, keySpec, ivSpec)
  //    cipher
  //  }
  //
  //  private def generateChaCha20Key() = {
  //    val KEY_LEN = 256 // bits
  //    val keyGen = KeyGenerator.getInstance("ChaCha20")
  //    keyGen.init(KEY_LEN, SecureRandom.getInstanceStrong)
  //    val secretKey = keyGen.generateKey
  //
  //    val secretKeySpec = new SecretKeySpec(secretKey.getEncoded, "ChaCha20")
  //    secretKeySpec
  //  }
}

package alpakka.sftp

import java.net.InetAddress
import java.nio.file.{Files, Paths}

import akka.actor.ActorSystem
import akka.stream.alpakka.ftp.scaladsl.Sftp
import akka.stream.alpakka.ftp.{FtpCredentials, FtpFile, SftpSettings}
import akka.stream.scaladsl.{FileIO, Keep, Sink, Source}
import akka.stream.{IOResult, ThrottleMode}
import akka.util.ByteString
import akka.{Done, NotUsed}
import alpakka.jms.JMSTextMessageProducerClient.logger
import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.transport.verification.PromiscuousVerifier
import net.schmizz.sshj.xfer.FileSystemFile

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object SFTPEcho {
  implicit val system = ActorSystem("SFTPEcho")
  implicit val executionContext = system.dispatcher

  val resourceFileName = "testfile.jpg"
  val resourceFilePath = Paths.get(s"./src/main/resources/$resourceFileName")

  val localTargetDir = Paths.get("/tmp/echo")

  val hostname =   "127.0.0.1"
  val port = 2222
  val username =  "echouser"
  val password =  "password"
  val credentials = FtpCredentials.create(username, password )

  val destinationDirName = "echo"

  //val identity = SftpIdentity.createFileSftpIdentity(pathToIdentityFile, privateKeyPassphrase)
  val sftpSettings = SftpSettings(InetAddress.getByName(hostname))
    .withPort(port)
    //.withSftpIdentity(identity)
    .withStrictHostKeyChecking(false)
    .withCredentials(credentials)


  def main(args: Array[String]) {

    //uploadFileBasic()

    removeAll().onComplete {
      case Success(_) =>
        println("Successfully cleaned ...")
        uploadClient()
        Thread.sleep(5000)
        downloadClient()
      case Failure(e) =>
        println(s"Failure: ${e.getMessage}. About to terminate...")
        system.terminate()
    }
  }

  def uploadClient() = {

    //mkdir("/", destinationDir, sftpSettings)

    println("Starting upload...")

    //Simulate unbounded stream of files to upload. Generate content in memory to avoid the overhead of
    //generating n test files on the filesystem
    Source(1 to 10)
      .throttle(1, 1.second, 1, ThrottleMode.shaping)
      .wireTap(number => logger.info(s"Upload file with TRACE_ID: $number"))
      .runForeach { each =>
       val result: Future[IOResult] = Source
      .single(ByteString(s"this is the file contents for: $each"))
      .runWith(uploadToPath(destinationDirName + s"/file_$each.txt"))
      result.onComplete(res => println(s"Client uploaded file with TRACE_ID: $each. Result: $res"))
    }
  }


  def createSshClientAndConnect() =
  {
    val sshClient = new SSHClient
    sshClient.addHostKeyVerifier(new PromiscuousVerifier) // to skip host verification

    sshClient.loadKnownHosts()
    sshClient.connect(hostname, port)
    sshClient.authPassword(username, password)
    sshClient
  }

  def uploadFileBasic() = {

    //mkdir("/upload", destinationDir, sftpSettings)

    val sshClient = createSshClientAndConnect()
    val sftpClient = sshClient.newSFTPClient()

      // backslash doesn't work when running unit tests from windows machine; also this makes configuring
      // destination dir more bulletproof, it can be "/outbox" or "/outbox/" or "\outbox" or "\outbox\"
      val targetFileOnServer = Paths.get(destinationDirName)
        .resolve(resourceFileName).toString().replace("\\", "/")

      // to prevent updating file attributes of uploaded file at destination SFTP server, by default
      // the library will update target file permissions and modify access time and modification time
      sftpClient.getFileTransfer().setPreserveAttributes(false)
      sftpClient.put(new FileSystemFile(resourceFilePath.toFile), targetFileOnServer)
    }



  //TODO Due to the nature of the ftp ls cmd, the download happens only...
  //A continous download would be nice
  def downloadClient() = {

    println("Starting download...")
    val fetchedFiles: Future[immutable.Seq[(String, IOResult)]] =
      listFiles(s"/$destinationDirName")
        .filter(ftpFile => ftpFile.isFile)
        .mapAsyncUnordered(parallelism = 5) { ftpFile =>
          fetch(ftpFile)
        }
        .runWith(Sink.seq)


    fetchedFiles
      .map { files: immutable.Seq[(String, IOResult)] =>
        println(s"Fetched: ${files.seq.size} files: " + files)
        files.filter { case (_, r) => r.status.isFailure }
      }
      .onComplete {
        case Success(errors) if errors.isEmpty =>
          println("All files fetched for this run")
        case Success(errors) =>
          println(s"Errors occurred: ${errors.mkString("\n")}")
        case Failure(exception) =>
          println(s"The stream failed with: ${exception.getCause}")
      }

    //TODO Add shutdown code
//    system.terminate()
//    system.whenTerminated.onComplete { _ =>
//      //stop docker image
//    }

  }




  private def mkdir(basePath: String, directoryName: String): Source[Done, NotUsed] =
    Sftp.mkdir(basePath, directoryName, sftpSettings)

  private def listFiles(basePath: String): Source[FtpFile, NotUsed] =
    Sftp.ls(basePath, sftpSettings)

  private def uploadToPath(path: String) = {
    Sftp.toPath(path, sftpSettings)
  }

  private def retrieveFromPath(path: String): Source[ByteString, Future[IOResult]] =
    Sftp.fromPath(path, sftpSettings)


  private def fetch(ftpFile: FtpFile) = {
    println(s"About to fetch file: $ftpFile")
    val localPath = localTargetDir.resolve(ftpFile.name)
    val fetchFile: Future[IOResult] = retrieveFromPath(ftpFile.path)
      .runWith(FileIO.toPath(localPath))
    fetchFile.map { ioResult =>
      (ftpFile.path, ioResult)
    }
  }


  private def fetchAndMove(ftpFile: FtpFile) = {
  }


  private def processAndMove(sourcePath: String,
                             destinationPath: FtpFile => String) = {
    Sftp
      .ls(sourcePath, sftpSettings)
      .flatMapConcat(ftpFile => Sftp.fromPath(ftpFile.path, sftpSettings).map((_, ftpFile)))
      .alsoTo(FileIO.toPath(Files.createTempFile("downloaded", "tmp")).contramap(_._1))
      .toMat(Sftp.move(destinationPath, sftpSettings).contramap(_._2))(Keep.right)
  }

  private def removeAll() = {
    val source = listFiles("/")
    val sink = Sftp.remove(sftpSettings)
    source.runWith(sink)
  }
}
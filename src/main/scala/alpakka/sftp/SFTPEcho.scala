package alpakka.sftp

import java.io.File
import java.net.InetAddress
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.alpakka.ftp.scaladsl.Sftp
import akka.stream.alpakka.ftp.{FtpCredentials, FtpFile, SftpSettings}
import akka.stream.scaladsl.{FileIO, Sink, Source}
import akka.stream.{IOResult, ThrottleMode}
import akka.util.ByteString
import akka.{Done, NotUsed}
import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.sftp.SFTPClient
import net.schmizz.sshj.transport.verification.PromiscuousVerifier
import net.schmizz.sshj.xfer.FileSystemFile
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}


/**
  * Implement an upload/download echo flow for the happy path, trying to use alpakka sftp features for everything
  *
  * Reproducer to show these issues:
  *  - Alpakka SFTP mkdir() and move() operations fail silently
  *  - Trying to use the native sshj rename() instead of SFTP.move() leads after around 85 elements to: net.schmizz.sshj.sftp.SFTPException
  * It looks as if this is an sshj issue, since sshj rm() works, see moveFileNative() below
  *
  * Remarks:
  *  - Prerequisite: start the docker SFTP server from: /docker/docker-compose.yml (eg by cmd line: docker-compose up -d sftp)
  *  - Log noise from libs is turned down in logback.xml
  *
  * TODOs
  *  - Implement failure scenarios during upload/download
  */
object SFTPEcho extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("SFTPEcho")
  implicit val executionContext = system.dispatcher

  val resourceFileName = "testfile.jpg"
  val resourceFilePath = Paths.get(s"./src/main/resources/$resourceFileName")

  val sftpDirName = "echo"
  val processedDirName = "processed"

  val hostname = "127.0.0.1"
  val port = 2222
  val username = "echouser"
  val password = "password"
  val credentials = FtpCredentials.create(username, password)

  //val identity = SftpIdentity.createFileSftpIdentity(pathToIdentityFile, privateKeyPassphrase)
  val sftpSettings = SftpSettings(InetAddress.getByName(hostname))
    .withPort(port)
    //.withSftpIdentity(identity)
    .withStrictHostKeyChecking(false)
    .withCredentials(credentials)

  var sshClient = createSshClientAndConnect()

  removeAll().onComplete {
    case Success(_) =>
      logger.info("Successfully cleaned...")
      createFoldersNative()
      uploadClient()
      downloadClient()
    case Failure(e) =>
      logger.info(s"Failure while cleaning: ${e.getMessage}. About to terminate...")
      system.terminate()
  }


  def uploadClient() = {
    logger.info("Starting upload...")

    Source(1 to 100)
      .throttle(10, 1.second, 10, ThrottleMode.shaping)
      .wireTap(number => logger.info(s"Upload file with TRACE_ID: $number"))
      .mapAsync(parallelism = 10) {
        each =>
          val result = Source
            //Generate file content in memory to avoid the overhead of generating n test files on the filesystem
            .single(ByteString(s"this is the file contents for: $each"))
            .runWith(uploadToPath(sftpDirName + s"/file_$each.txt"))
          result.onComplete(res => logger.info(s"Client uploaded file with TRACE_ID: $each. Result: $res"))
          result
      }
      .runWith(Sink.ignore)
  }


  def downloadClient(): Unit = {
    Thread.sleep(5000) //wait to get some files
    logger.info("Starting download run...")

    val fetchedFiles: Future[immutable.Seq[(String, IOResult)]] =
      listFiles(s"/$sftpDirName")
        .take(10) //Try to batch
        .filter(ftpFile => ftpFile.isFile)
        .mapAsyncUnordered(parallelism = 5)(ftpFile => fetchAndMove(ftpFile))
        .runWith(Sink.seq)

    fetchedFiles
      .map { files: immutable.Seq[(String, IOResult)] =>
        logger.info(s"Fetched: ${files.seq.size} files: " + files)
        files.filter { case (_, r) => r.status.isFailure }
      }
      .onComplete {
        case Success(errors) if errors.isEmpty =>
          logger.info("All files fetched for this run. About to start next run.")
          downloadClient() //Try to do a continuous download
        case Success(errors) =>
          logger.info(s"Errors occurred: ${errors.mkString("\n")}")
          system.terminate()
        case Failure(exception) =>
          logger.info(s"The stream failed with: ${ExceptionUtils.getRootCause(exception)}")
          system.terminate()
      }
  }


  private def fetchAndMove(ftpFile: FtpFile) = {

    val localFile = File.createTempFile(ftpFile.name, ".tmp.client")
    //localFile.deleteOnExit()
    val localPath = localFile.toPath
    logger.info(s"About to fetch file: $ftpFile to local path: $localPath")

    val fetchFile: Future[IOResult] = retrieveFromPath(ftpFile.path)
      .runWith(FileIO.toPath(localPath))
    fetchFile.map { ioResult =>
      //Fails silently
      //Sftp.move((ftpFile) => s"$sftpDirName/$processedDirName/", sftpSettings)

      moveFileNative(ftpFile)
      (ftpFile.path, ioResult)
    }
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


  //TODO This just fails silently, no folders are created
  private def createFolders() = {
    mkdir(sftpDirName, processedDirName)
  }

  //works
  private def createFoldersNative() = {
    val sftpClient = newSFTPClient()

    try {
      if (sftpClient.statExistence(s"$sftpDirName/$processedDirName") == null) {
        sftpClient.mkdir(s"$sftpDirName/$processedDirName")
      } else {
        sftpClient.rmdir(s"$sftpDirName/$processedDirName")
        sftpClient.mkdir(s"$sftpDirName/$processedDirName")
      }
    } finally
      sftpClient.close()
  }

  private def moveFileNative(ftpFile: FtpFile) = {
    val sftpClient = newSFTPClient()

    try {
      //TODO moving/renaming via sshj leads to unknown (resource?)-exception in sshj :-(
      //sftpClient.rename(ftpFile.path, s"$sftpDirName/$processedDirName/${ftpFile.name}")

      //rm works
      sftpClient.rm(ftpFile.path)
    } finally
      sftpClient.close()
  }


  //works
  private def removeAll() = {
    val source = listFiles("/")
    val sink = Sftp.remove(sftpSettings)
    source.runWith(sink)
  }

  //Passing path of file to be removed does not work
  //This works, but feels clumsy
  private def remove(path: String) = {
    val source = listFiles("/")
      .filter(each => each.path.equals(path))
    val sink = Sftp.remove(sftpSettings)
    source.runWith(sink)
  }

  //works
  private def uploadFileNative() = {
    val sftpClient = newSFTPClient()

    try {
      val targetFileOnServer = Paths.get(sftpDirName)
        .resolve(resourceFileName).toString.replace("\\", "/")

      // to prevent updating file attributes of uploaded file at destination SFTP server, by default
      // the library will update target file permissions and modify access time and modification time
      sftpClient.getFileTransfer.setPreserveAttributes(false)
      sftpClient.put(new FileSystemFile(resourceFilePath.toFile), targetFileOnServer)
    } finally
      sftpClient.close()
  }

  private def newSFTPClient(): SFTPClient = {
    if (!sshClient.isConnected) {
      sshClient = createSshClientAndConnect()
    }
    sshClient.newSFTPClient()
  }

  private def createSshClientAndConnect(): SSHClient = {
    val sshClient = new SSHClient
    sshClient.addHostKeyVerifier(new PromiscuousVerifier) // to skip host verification

    sshClient.loadKnownHosts()
    sshClient.connect(hostname, port)
    sshClient.authPassword(username, password)
    sshClient
  }
}
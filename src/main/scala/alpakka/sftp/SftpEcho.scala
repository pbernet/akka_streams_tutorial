package alpakka.sftp

import java.net.InetAddress
import java.nio.file.{Files, Paths}

import akka.actor.ActorSystem
import akka.stream.alpakka.ftp.scaladsl.Sftp
import akka.stream.alpakka.ftp.{FtpCredentials, FtpFile, SftpSettings}
import akka.stream.scaladsl.{FileIO, RunnableGraph, Sink, Source}
import akka.stream.{IOResult, ThrottleMode}
import akka.util.ByteString
import akka.{Done, NotUsed}
import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.sftp.SFTPClient
import net.schmizz.sshj.transport.verification.PromiscuousVerifier
import net.schmizz.sshj.xfer.FileSystemFile
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}


/**
  * Implement an upload/download echo flow for the happy path, trying to use alpakka sftp features for everything
  *
  * Prerequisite:
  *  - Start the docker SFTP server from: /docker/docker-compose.yml (eg by cmd line: docker-compose up -d atmoz_sftp)
  *
  * Reproducer to show these issues:
  *  - TODO Alpakka SFTP mkdir() operations fails silently
  *  - TODO Method processAndMove hangs after 30 elements
  *
  * Remarks:
  *  - Log noise from sshj lib is turned down in logback.xml
  *  - Implement failure scenarios during upload/download
  *
  * Doc:
  * https://doc.akka.io/docs/alpakka/current/ftp.html
  *
  */
object SftpEcho extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("SftpEcho")
  implicit val executionContext = system.dispatcher

  //we need a sub folder due to permissions set on on the atmoz_sftp docker image
  val sftpRootDir = "echo"
  val processedDir = "processed"

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
            .runWith(uploadToPath(sftpRootDir + s"/file_$each.txt"))
          result.onComplete(res => logger.info(s"Client uploaded file with TRACE_ID: $each. Result: $res"))
          result
      }
      .runWith(Sink.ignore)
  }


  def downloadClient(): Unit = {
    Thread.sleep(5000) //wait to get some files
    logger.info("Starting download run...")

    processAndMove(s"/$sftpRootDir", (file: FtpFile) => s"/$sftpRootDir/$processedDir/${file.name}", sftpSettings)
      .run()
  }

  //TODO This hangs after 30 elements
  def processAndMove(sourcePath: String,
                     destinationPath: FtpFile => String,
                     sftpSettings: SftpSettings): RunnableGraph[NotUsed] =
    Sftp
      .ls(sourcePath, sftpSettings)
      .flatMapConcat(ftpFile => Sftp.fromPath(ftpFile.path, sftpSettings).map((_, ftpFile)))
      .wireTap(each => logger.info(s"About to process: $each"))
      .alsoTo(FileIO.toPath(Files.createTempFile("downloaded", "tmp")).contramap(_._1))
      .wireTap(each => logger.info(s"About to move: $each"))
      .to(Sftp.move(destinationPath, sftpSettings).contramap(_._2))


  private def mkdir(basePath: String, directoryName: String): Source[Done, NotUsed] =
    Sftp.mkdir(basePath, directoryName, sftpSettings)

  private def listFiles(basePath: String): Source[FtpFile, NotUsed] =
    Sftp.ls(basePath, sftpSettings)

  private def uploadToPath(path: String) = {
    Sftp.toPath(path, sftpSettings)
  }

  private def retrieveFromPath(path: String): Source[ByteString, Future[IOResult]] =
    Sftp.fromPath(path, sftpSettings)


  //TODO This fails silently, no folders are created
  private def createFolders() = {
    mkdir(sftpRootDir, processedDir)
  }

  //works
  private def createFoldersNative() = {
    val sftpClient = newSftpClient()

    try {
      if (sftpClient.statExistence(s"$sftpRootDir/$processedDir") == null) {
        sftpClient.mkdir(s"$sftpRootDir/$processedDir")
      } else {
        sftpClient.rmdir(s"$sftpRootDir/$processedDir")
        sftpClient.mkdir(s"$sftpRootDir/$processedDir")
      }
    } finally
      sftpClient.close()
  }

  //works
  private def removeAll() = {
    val source = listFiles("/")
    val sink = Sftp.remove(sftpSettings)
    source.runWith(sink)
  }

  //Passing the path of file to be removed does not work
  //This works, but feels clumsy
  private def remove(path: String) = {
    val source = listFiles("/")
      .filter(each => each.path.equals(path))
    val sink = Sftp.remove(sftpSettings)
    source.runWith(sink)
  }

  //works
  private def uploadFileNative() = {
    val resourceFileName = "testfile.jpg"
    val resourceFilePath = Paths.get(s"./src/main/resources/$resourceFileName")
    val sftpClient = newSftpClient()

    try {
      val targetFileOnServer = Paths.get(sftpRootDir)
        .resolve(resourceFileName).toString.replace("\\", "/")

      // to prevent updating file attributes of uploaded file at destination SFTP server, by default
      // the library will update target file permissions and modify access time and modification time
      sftpClient.getFileTransfer.setPreserveAttributes(false)
      sftpClient.put(new FileSystemFile(resourceFilePath.toFile), targetFileOnServer)
    } finally
      sftpClient.close()
  }

  private def newSftpClient(): SFTPClient = {
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
package alpakka.sftp

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
import net.schmizz.sshj.transport.verification.PromiscuousVerifier
import net.schmizz.sshj.xfer.FileSystemFile
import org.apache.commons.lang3.exception.ExceptionUtils

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}


/**
  * Reproducer to show these issues:
  * - Alpakka mkdir and move operations fail silently
  * - After around 85 files the download fails with: net.schmizz.sshj.sftp.SFTPException: Failure
  *
  * Remarks:
  * - Start the SFTP server from: /docker/docker-compose.yml with: docker-compose up -d sftp
  * - Try to use as many alpakka features as possible and thus do the cleaning manually
  *
  */
object SFTPEcho extends App {
  implicit val system = ActorSystem("SFTPEcho")
  implicit val executionContext = system.dispatcher

  val resourceFileName = "testfile.jpg"
  val resourceFilePath = Paths.get(s"./src/main/resources/$resourceFileName")

  val sftpDirName = "echo"
  val processedDirName = "processed"
  val localTargetDir = Paths.get(s"/tmp/$sftpDirName")

  val hostname =   "127.0.0.1"
  val port = 2222
  val username =  "echouser"
  val password =  "password"
  val credentials = FtpCredentials.create(username, password )

  //val identity = SftpIdentity.createFileSftpIdentity(pathToIdentityFile, privateKeyPassphrase)
  val sftpSettings = SftpSettings(InetAddress.getByName(hostname))
    .withPort(port)
    //.withSftpIdentity(identity)
    .withStrictHostKeyChecking(false)
    .withCredentials(credentials)


   removeAll().onComplete {
      case Success(_) =>
        println("Successfully cleaned...")
        createFoldersNative()
        uploadClient()
        Thread.sleep(5000) //wait to get some files
        downloadClient()
      case Failure(e) =>
        println(s"Failure while cleaning: ${e.getMessage}. About to terminate...")
        system.terminate()
    }


  def uploadClient() = {
    println("Starting upload...")

    //Simulate unbounded stream of files to upload
    //Generate file content in memory to avoid the overhead of generating n test files on the filesystem
    Source(1 to 100)
      .throttle(10, 1.second, 10, ThrottleMode.shaping)
      .wireTap(number => println(s"Upload file with TRACE_ID: $number"))
      .runForeach { each =>
       val result: Future[IOResult] = Source
      .single(ByteString(s"this is the file contents for: $each"))
      .runWith(uploadToPath(sftpDirName + s"/file_$each.txt"))
      result.onComplete(res => println(s"Client uploaded file with TRACE_ID: $each. Result: $res"))
    }
  }


  def downloadClient() : Unit= {
    println("Starting download run...")

    val fetchedFiles: Future[immutable.Seq[(String, IOResult)]] =
      listFiles(s"/$sftpDirName")
        .take(10) //Try to batch
        .filter(ftpFile => ftpFile.isFile)
        .mapAsyncUnordered(parallelism = 5) (ftpFile => fetchAndMove(ftpFile))
        .runWith(Sink.seq)

    fetchedFiles
      .map { files: immutable.Seq[(String, IOResult)] =>
        println(s"Fetched: ${files.seq.size} files: " + files)
        files.filter { case (_, r) => r.status.isFailure }
      }
      .onComplete {
        case Success(errors) if errors.isEmpty =>
          println("All files fetched for this run. About to start next run.")
          downloadClient() //Try to do a continuous download
        case Success(errors) =>
          println(s"Errors occurred: ${errors.mkString("\n")}")
          system.terminate()
        case Failure(exception) =>
          println(s"The stream failed with: ${ExceptionUtils.getRootCause(exception)}")
          system.terminate()
      }
  }


  private def fetchAndMove(ftpFile: FtpFile) = {

    val localPath = localTargetDir.resolve(ftpFile.name)
    println(s"About to fetch file: $ftpFile to local path: $localPath")

    val fetchFile: Future[IOResult] = retrieveFromPath(ftpFile.path)
      .runWith(FileIO.toPath(localPath))
    fetchFile.map { ioResult =>
      //Fails silently
      //Sftp.move((ftpFile) => s"$sftpDirName/processed/", sftpSettings)

      //works
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
    mkdir(sftpDirName, processedDirName )
  }

  //works
  private def createFoldersNative() = {
    val sshClient = createSshClientAndConnect()
    val sftpClient = sshClient.newSFTPClient()

    sftpClient.rmdir(s"$sftpDirName/$processedDirName")
    sftpClient.mkdir(s"$sftpDirName/$processedDirName")
    sftpClient.close()
  }

  //works
  private def moveFileNative(ftpFile: FtpFile) = {
    val sshClient = createSshClientAndConnect()
    val sftpClient = sshClient.newSFTPClient()

    sftpClient.rename(ftpFile.path, s"$sftpDirName/$processedDirName/${ftpFile.name}")
    sftpClient.close()
  }


  private def removeAll() = {
    val source = listFiles("/")
    val sink = Sftp.remove(sftpSettings)
    source.runWith(sink)
  }

  //works
  private def uploadFileNative() = {
    val sshClient = createSshClientAndConnect()
    val sftpClient = sshClient.newSFTPClient()

    val targetFileOnServer = Paths.get(sftpDirName)
      .resolve(resourceFileName).toString().replace("\\", "/")

    // to prevent updating file attributes of uploaded file at destination SFTP server, by default
    // the library will update target file permissions and modify access time and modification time
    sftpClient.getFileTransfer().setPreserveAttributes(false)
    sftpClient.put(new FileSystemFile(resourceFilePath.toFile), targetFileOnServer)
  }

  private def createSshClientAndConnect() =
  {
    val sshClient = new SSHClient
    sshClient.addHostKeyVerifier(new PromiscuousVerifier) // to skip host verification

    sshClient.loadKnownHosts()
    sshClient.connect(hostname, port)
    sshClient.authPassword(username, password)
    sshClient
  }
}
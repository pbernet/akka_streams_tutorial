package alpakka.file

import alpakka.file.uploader.DirectoryListener
import org.apache.commons.io.FileUtils
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEachTestData, TestData}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.{Files, Path, Paths}
import scala.util.Random

final class DirectoryListenerSpec extends AsyncWordSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEachTestData {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  var listener: DirectoryListener = _
  var tmpRootDir: Path = _
  var uploadDir: Path = _
  var processedDir: Path = _

  "DirectoryListener" should {

    "detect_files_onstartup_in_upload_dir" in {

      Thread.sleep(3000)
      listener.countFilesProcessed() shouldEqual 2
    }

    "detect_added_file_in_upload_dir" in {
      copyTestFileToDir(listener, uploadDir)
      
      Thread.sleep(3000)
      listener.countFilesProcessed() shouldEqual 2 + 1
    }

    "detect_added_file_in_uploadSubDir" in {
      copyTestFileToDir(listener, uploadDir.resolve("subdir"))

      Thread.sleep(3000)
      listener.countFilesProcessed() shouldEqual 2 + 1
    }

    // TODO Add test with nested dropped dirs
    "detect_added_dir_with_files" in {
      val tmpDir = Files.createTempDirectory("tmp")
      val sourcePath = Paths.get("src/main/resources/testfile.jpg")
      val targetPath = tmpDir.resolve(createUniqueFileName(sourcePath.getFileName))
      val targetPath2 = tmpDir.resolve(createUniqueFileName(sourcePath.getFileName))
      Files.copy(sourcePath, targetPath)
      Files.copy(sourcePath, targetPath2)
      FileUtils.copyDirectory(tmpDir.toFile, uploadDir.resolve("dirWithFiles").toFile)

      Thread.sleep(3000)
      listener.countFilesProcessed() shouldEqual 2 + 2
    }
  }

  override protected def beforeEach(testData: TestData): Unit = {
    logger.info(s"Starting test: ${testData.name}")

    tmpRootDir = Files.createTempDirectory(testData.text)
    logger.info(s"Created tmp dir: $tmpRootDir")

    uploadDir = tmpRootDir.resolve("upload")
    processedDir = tmpRootDir.resolve("processed")
    Files.createDirectories(uploadDir)
    Files.createDirectories(uploadDir.resolve("subdir"))
    Files.createDirectories(processedDir)

    // Populate dir BEFORE startup
    val sourcePath = Paths.get("src/main/resources/testfile.jpg")
    val targetPath = tmpRootDir.resolve("upload").resolve(createUniqueFileName(sourcePath.getFileName))
    Files.copy(sourcePath, targetPath)

    val targetPathSubDir = tmpRootDir.resolve("upload/subdir").resolve(createUniqueFileName(sourcePath.getFileName))
    Files.copy(sourcePath, targetPathSubDir)

    listener = DirectoryListener(uploadDir, processedDir)
  }

  override protected def afterEach(testData: TestData): Unit = {
    logger.info(s"Cleaning up after test: ${testData.name}")
    listener.stop()
    FileUtils.deleteDirectory(tmpRootDir.toFile)
  }

  private def copyTestFileToDir(listener: DirectoryListener, target: Path) = {
    val sourcePath = Paths.get("src/main/resources/testfile.jpg")
    val targetPath = target.resolve(createUniqueFileName(createUniqueFileName(sourcePath.getFileName)))
    Files.copy(sourcePath, targetPath)
  }

  private def createUniqueFileName(fileName: Path)  = {
    val splitted = fileName.toString.split('.').map(_.trim)
    Paths.get(splitted.head + Random.nextInt() + "." + splitted.reverse.head)
  }
}

package alpakka.file

import alpakka.file.uploader.DirectoryWatcher
import org.apache.commons.io.FileUtils
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEachTestData, TestData}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.{Files, Path, Paths}
import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.util.Random

/**
  * Designed as IT test on purpose to demonstrate
  * the realistic usage of [[DirectoryWatcher]]
  * Hence we:
  *  - create the dir structure and copy files before each test
  *  - clean up dir structure after each test
  */
final class DirectoryWatcherSpec extends AsyncWordSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEachTestData with Eventually {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val defaultTimeout: FiniteDuration = 5.seconds
  implicit val patience: PatienceConfig = PatienceConfig(timeout = Span(20, Seconds))
  var tmpRootDir: Path = _
  var uploadDir: Path = _
  var processedDir: Path = _

  case class WatcherFixture(watcher: DirectoryWatcher) {
    def withWatcher[T](testCode: DirectoryWatcher => T): T = {
      try {
        testCode(watcher)
      } finally {
        Await.result(watcher.stop(), defaultTimeout)
      }
    }
  }

  "DirectoryWatcher" should {
    "detect_files_on_startup_in_parent_dir" in {
      WatcherFixture(DirectoryWatcher(uploadDir, processedDir))
        .withWatcher { watcher =>
          eventually {
            watcher.countFilesProcessed() shouldBe 2
          }
        }
    }


    "detect_added_file_at_runtime_in_parent_dir" in {
      copyTestFileToDir(uploadDir)
      WatcherFixture(DirectoryWatcher(uploadDir, processedDir))
        .withWatcher { watcher =>
          eventually {
            watcher.countFilesProcessed() shouldBe 2 + 1
          }
        }
    }

    "detect_added_files_at_runtime_in_sub_dir" in {
      copyTestFileToDir(uploadDir.resolve("subdir"))
      WatcherFixture(DirectoryWatcher(uploadDir, processedDir))
        .withWatcher { watcher =>
          eventually {
            watcher.countFilesProcessed() shouldBe 2 + 1
          }
        }
    }

    "detect_added_nested_subdir_at_runtime_with_files_in_subdir" in {
        val tmpDir = Files.createTempDirectory("tmp")
        val sourcePath = Paths.get("src/main/resources/testfile.jpg")
        val targetPath = tmpDir.resolve(createUniqueFileName(sourcePath.getFileName))
        val targetPath2 = tmpDir.resolve(createUniqueFileName(sourcePath.getFileName))
        Files.copy(sourcePath, targetPath)
        Files.copy(sourcePath, targetPath2)
        val targetDir = Files.createDirectories(uploadDir.resolve("subdir").resolve("nestedDirWithFiles"))
        FileUtils.copyDirectory(tmpDir.toFile, targetDir.toFile)
      WatcherFixture(DirectoryWatcher(uploadDir, processedDir))
        .withWatcher { watcher =>
          eventually {
            watcher.countFilesProcessed() shouldBe 2 + 2
          }
        }
    }

    "handle_large_number_of_files_in_parent_dir" in {
      (1 to 1000).foreach(_ => copyTestFileToDir(uploadDir))
      WatcherFixture(DirectoryWatcher(uploadDir, processedDir))
        .withWatcher { watcher =>
          eventually {
            watcher.countFilesProcessed() shouldBe 2 + 1000
          }
        }
    }

    "handle_invalid_parent_directory_path" in {
      val invalidParentDir = Paths.get("/path/to/non-existent/directory")
      val processedDir = Files.createTempDirectory("processed")

      the[IllegalArgumentException] thrownBy {
        DirectoryWatcher(invalidParentDir, processedDir)
      } should have message s"Invalid upload directory path: $invalidParentDir"
    }
  }

  override protected def beforeEach(testData: TestData): Unit = {
    logger.info(s"Starting test: ${testData.name}")

    def withDirectoryCreation[T](action: => T): T = {
      try {
        tmpRootDir = Files.createTempDirectory(testData.text)
        logger.info(s"Created tmp root dir: $tmpRootDir")

        uploadDir = tmpRootDir.resolve("upload")
        processedDir = tmpRootDir.resolve("processed")

        Files.createDirectories(uploadDir)
        Files.createDirectories(uploadDir.resolve("subdir"))
        Files.createDirectories(processedDir)

        action
      } catch {
        case ex: Exception =>
          logger.error(s"Failed to set up test directories: ${ex.getMessage}")
          if (tmpRootDir != null) {
            FileUtils.deleteDirectory(tmpRootDir.toFile)
          }
          throw ex
      }
    }

    withDirectoryCreation {
      // Populate dirs BEFORE startup
      try {
        copyTestFileToDir(uploadDir)
        copyTestFileToDir(uploadDir.resolve("subdir"))
      } catch {
        case ex: Exception =>
          logger.error(s"Failed to copy test files: ${ex.getMessage}")
          throw ex
      }
    }
  }

  override protected def afterEach(testData: TestData): Unit = {
    logger.info(s"Cleaning up after test: ${testData.name}")
    FileUtils.deleteDirectory(tmpRootDir.toFile)
    logger.info(s"Finished test: ${testData.name}")
  }

  private def copyTestFileToDir(target: Path) = {
    val sourcePath = Paths.get("src/main/resources/testfile.jpg")
    val targetPath = target.resolve(createUniqueFileName(createUniqueFileName(sourcePath.getFileName)))
    Files.copy(sourcePath, targetPath)
  }
  private def createUniqueFileName(fileName: Path) = {
    val parts = fileName.toString.split('.').map(_.trim)
    Paths.get(s"${parts.head}${Random.nextInt()}.${parts.reverse.head}")
  }
}
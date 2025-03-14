import alpakka.file.uploader.DirectoryWatcher
import io.gatling.core.Predef.*
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.Session
import io.gatling.core.structure.{ScenarioBuilder, ScenarioContext}
import io.gatling.core.util.NameGen

import java.nio.file.{Files, Paths}
import java.util.UUID
import scala.concurrent.duration.*

/**
  * Show the use of Gatling ActionBuilder to create a custom action
  * In our case to generate files to be picked up by the [[DirectoryWatcher]]
  *
  * Run from terminal:
  * sbt 'Gatling/testOnly DirectoryWatcherSimulation -- --no-reports'
  */
class DirectoryWatcherSimulation extends Simulation {
  private val rootDir = "/tmp/directory-watcher-simulation"
  private val uploadDir = Paths.get(rootDir, "upload")
  private val processedDir = Paths.get(rootDir, "processed")

  setupDirectories()
  val watcher = DirectoryWatcher(uploadDir, processedDir)

  private def setupDirectories(): Unit = {
    Files.createDirectories(Paths.get(rootDir))

    if (!Files.exists(uploadDir)) {
      Files.createDirectories(uploadDir)
      println(s"Created temporary upload directory at: $uploadDir")
    }

    if (!Files.exists(processedDir)) {
      Files.createDirectories(processedDir)
      println(s"Created temporary processed directory at: $processedDir")
    }
  }

  // Custom action to generate unique files
  class FileGenerationAction(next: Action) extends Action with NameGen {

    override def name: String = genName("fileGeneration")

    override def execute(session: Session): Unit = {
      val uuid = UUID.randomUUID().toString
      val fileName = s"generated_file_$uuid.txt"
      val filePath = uploadDir.resolve(fileName)
      val content = "This is a generated file."

      Files.write(filePath, content.getBytes)
      println(s"Generated file: $filePath")

      next ! session
    }
  }

  // Custom action builder to wrap the FileGenerationAction
  class FileGenerationActionBuilder extends ActionBuilder {
    override def build(ctx: ScenarioContext, next: Action): Action = {
      new FileGenerationAction(next)
    }
  }

  class CountFilesProcessedAction(watcher: DirectoryWatcher, next: Action) extends Action {
    override def name: String = "CountFilesProcessedAction"

    override def execute(session: Session): Unit = {
      val processedFilesCount = watcher.countFilesProcessed()
      println(s"Total files processed: $processedFilesCount")
      next ! session
    }
  }

  class CountFilesProcessedActionBuilder(watcher: DirectoryWatcher) extends ActionBuilder {
    override def build(ctx: ScenarioContext, next: Action): Action = {
      new CountFilesProcessedAction(watcher, next)
    }
  }

  private val scn: ScenarioBuilder = scenario("DirectoryWatcher")
    .exec(new FileGenerationActionBuilder)
    .exec { session =>
      println("File Generation completed")
      session
    }
    .exec(new CountFilesProcessedActionBuilder(watcher))
    .exec { session =>
      println("Scenario completed")
      session
    }

  setUp(
    scn.inject(
      nothingFor(2.seconds),
      atOnceUsers(2),
      rampUsers(10) during 10.seconds
    )
  ).assertions(
    global.responseTime.max.lt(5000),
    global.successfulRequests.percent.is(100)
  ).maxDuration(30.seconds) // For now stop like this
}
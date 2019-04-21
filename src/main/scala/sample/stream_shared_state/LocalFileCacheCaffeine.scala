package sample.stream_shared_state

import java.io.File
import java.net.URI
import java.nio.file.{Path, Paths}
import java.util
import java.util.concurrent.ThreadLocalRandom

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorAttributes, ActorMaterializer, Supervision, ThrottleMode}
import com.github.benmanes.caffeine
import com.github.blemale.scaffeine.{Cache, Scaffeine}
import org.apache.commons.io.FileUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal


/**
  * Implement a local file cache with caffeine
  * Use scaffeine wrapper for convenience:
  * https://github.com/blemale/scaffeine
  * Use FileServer as faulty HTTP download mock
  *
  * Use case:
  * Avoid duplicate file downloads, thus keep them locally
  * Process a stream of messages with reoccurring IDs
  * For the first ID download a .zip file and add it to the local file cache
  * For each subsequent IDs try to load from local file cache
  * On downstream error messages need to be kept longer
  * On system restart: read all files from filesystem (ordered by lastModified)
  *
  * TODO
  * Multiple logback.xml are on the classpath, read from THIS one in project resources
  *
  */
object LocalFileCacheCaffeine {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val deciderFlow: Supervision.Decider = {
    case NonFatal(e) =>
      logger.info(s"Stream failed with: $e, going to resume")
      Supervision.Restart
    case _ => Supervision.Stop
  }

  val scaleFactor = 1 //Raise to stress more
  val evictionTime = 10.seconds
  val evictionTimeOnError = 60.seconds
  val localFileCache = Paths.get(System.getProperty("java.io.tmpdir")).resolve("localFileCache")

  FileUtils.forceMkdir(localFileCache.toFile)
  //Comment out to start with empty local file cache
  //FileUtils.cleanDirectory(localFileCache.toFile)


  val writer = new caffeine.cache.CacheWriter[Int, Path] {
    override def write(key: Int, value: Path): Unit = {
      logger.debug(s"Writing to cache id: $key")
    }

    override def delete(key: Int, value: Path, cause: caffeine.cache.RemovalCause): Unit = {
      logger.info(s"Deleting from storage: $key because of: $cause")
      val destinationFile = localFileCache.resolve(value)
      FileUtils.deleteQuietly(destinationFile.toFile)
    }
  }

  val cache: Cache[Int, Path] =
    Scaffeine()
      .recordStats()
      .expireAfter[Int, Path]((_, _) => evictionTime, (_, _, _) => evictionTimeOnError, (_, _, _) => evictionTime)
      .maximumSize(500)
      .writer(writer)
      .build[Int, Path]()

  //Do "manual loading" for now
  val loadedResults: util.List[File] = new FileLister().run(localFileCache.toFile)
  loadedResults.forEach { each: File =>
    logger.debug(s"Add file: ${each.getName} with lastModified: ${each.lastModified()}")
    cache.put(each.getName.dropRight(4).toInt, each.toPath)
  }

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("LocalFileCache")
    implicit val materializer = ActorMaterializer()

    case class Message(group: Int, id: Int, file: File)

    //Within three groups: Generate random messages with IDs between 0/100
    val messages = List.fill(1000)(Message(ThreadLocalRandom.current.nextInt(0, 2), ThreadLocalRandom.current.nextInt(0, 100 * scaleFactor), null))


    val downloadFlow: Flow[Message, Message, NotUsed] = Flow[Message]
      .mapAsync(5) { message =>

        def processNext(message: Message): Message = {
          if (cache.getIfPresent(message.id).isDefined) {
            logger.info("Cache hit for ID: " + message.id)
            message
          } else {
            logger.info("Cache miss - download for ID: " + message.id)
            var downloadedFile: Path = null
            try {
              val destinationFile = localFileCache.resolve(Paths.get(message.id.toString + ".zip"))
              val url = new URI("http://127.0.0.1:6001/downloadflaky/" + message.id.toString)
              downloadedFile = new DownloaderRetry().download(url, destinationFile)
              cache.put(message.id, downloadedFile)
            } catch {
              case e@(_: Throwable) => {
                logger.error("Ex during download. " + e)
                throw e
              }
            }
            Message(message.group, message.id, downloadedFile.toFile)
          }
        }

        Future(processNext(message))

      }

    val faultyDownstreamFlow: Flow[Message, Message, NotUsed] = Flow[Message]
      .map { message: Message =>
        try {
          if (message.group == 0) throw new RuntimeException(s"Simulate error for message: ${message.id} in group 0")
        } catch {
          case e@(_: RuntimeException) => {
            //Force an update (= replacement of value) to extend cache time
            cache.put(message.id, message.file.toPath)
            logger.info(s"Extend eviction time for id: ${message.id} for group: ${message.group}")
          }
        }
        message
      }

    Source(messages)
      .throttle(1 * scaleFactor, 1.second, 2 * scaleFactor, ThrottleMode.shaping)
      .via(downloadFlow)
      .via(faultyDownstreamFlow)
      .withAttributes(ActorAttributes.supervisionStrategy(deciderFlow))
      .runWith(Sink.ignore)
  }
}

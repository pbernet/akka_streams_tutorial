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
  * Use scaffeine wrapper for convenience: https://github.com/blemale/scaffeine
  * Use FileServer as faulty HTTP download mock
  * Monitor localFileCache with:  watch ls -ltr
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
  * Implement download barrier to meet non-idempotent behaviour of the real download server,
  * which does not allow two parallel requests with the same ID but different group
  * 
  * Multiple logback.xml are on the classpath, read from THIS one in project resources
  *
  */
object LocalFileCacheCaffeine {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val deciderFlow: Supervision.Decider = {
    case NonFatal(e) =>
      logger.info(s"Stream failed with: $e, going to restart")
      logger.debug(s"Stream failed with: $e, going to restart", e)
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
      logger.debug(s"Writing to cache ID: $key")
    }

    override def delete(key: Int, value: Path, cause: caffeine.cache.RemovalCause): Unit = {
      logger.info(s"Deleting from storage for ID: $key because of: $cause")
      val destinationFile = localFileCache.resolve(value)
      FileUtils.deleteQuietly(destinationFile.toFile)
    }
  }

  val cache: Cache[Int, Path] =
    Scaffeine()
      .recordStats()
      .expireAfter[Int, Path]((_, _) => evictionTime, (_, _, _) => evictionTimeOnError, (_, _, _) => evictionTime)
      .maximumSize(1000)
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

    case class Message(group: Int, id: Int, file: Path)

    //Within three groups: Generate random messages with IDs between 0/100, set to 5 to show "download barrier" issue
    val messages = List.fill(10000)(Message(ThreadLocalRandom.current.nextInt(0, 2), ThreadLocalRandom.current.nextInt(0, 100 * scaleFactor), null))


    val downloadFlow: Flow[Message, Message, NotUsed] = Flow[Message]
      .mapAsyncUnordered(5) { message =>

        def processNext(message: Message): Message = {
          if (cache.getIfPresent(message.id).isDefined) {
            val value = cache.getIfPresent(message.id).get
            logger.info(s"Cache hit for ID: ${message.id}")
            Message(message.group, message.id, value)
          } else {
            logger.info(s"Cache miss for ID: ${message.id} - download...")
            var downloadedFile: Path = null

              val destinationFile = localFileCache.resolve(Paths.get(message.id.toString + ".zip"))
              val url = new URI("http://127.0.0.1:6001/downloadflaky/" + message.id.toString)
              downloadedFile = new DownloaderRetry().download(url, destinationFile)
              cache.put(message.id, downloadedFile)
              Message(message.group, message.id, downloadedFile)
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
            logger.info(s"Extend eviction time for ID: ${message.id} from group: ${message.group}")
            cache.put(message.id, message.file)
          }
        }
        message
      }

    Source(messages)
      .throttle(1 * scaleFactor, 1.second, 2 * scaleFactor, ThrottleMode.shaping)
      //Try to go parallel on the ID
//      .groupBy(2, _.id % 2)
      .via(downloadFlow)
      .via(faultyDownstreamFlow)
//      .mergeSubstreams
      .withAttributes(ActorAttributes.supervisionStrategy(deciderFlow))
      .runWith(Sink.ignore)
  }
}

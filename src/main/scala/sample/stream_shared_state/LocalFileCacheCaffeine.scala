package sample.stream_shared_state

import java.net.URI
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Files, Path, Paths}
import java.util
import java.util.concurrent.ThreadLocalRandom

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, MergePrioritized, Sink, Source}
import akka.stream.{ActorAttributes, Supervision, ThrottleMode}
import com.github.benmanes.caffeine
import com.github.benmanes.caffeine.cache.CacheWriter
import com.github.blemale.scaffeine.{Cache, Scaffeine}
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.http.client.HttpResponseException
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal

/**
  * Use case:
  *  - Process a stream of messages with reoccurring TRACE_ID
  *  - For the first TRACE_ID download a .zip file from the FileServer and add the path to the cache
  *  - For subsequent TRACE_IDs try to fetch from the cache to avoid duplicate downloads per TRACE_ID
  *  - On downstream error: the file needs to be kept longer in the cache
  *  - On restart: read all files from filesystem to cache
  *
  * Before running this class: start [[alpakka.env.FileServer]] to simulate non idempotent responses
  * Monitor localFileCache dir with:  watch ls -ltr
  *
  * Doc:
  *  - Caffeine: https://github.com/ben-manes/caffeine
  *  - Scala wrapper scaffeine for type convenience: https://github.com/blemale/scaffeine
  */
object LocalFileCacheCaffeine {
  implicit val system = ActorSystem("LocalFileCacheCaffeine")
  implicit val executionContext = system.dispatcher
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val deciderFlow: Supervision.Decider = {
    case NonFatal(e) =>
      val rootCause = ExceptionUtils.getRootCause(e)
      logger.error(s"Stream failed with: $rootCause, going to restart")
      logger.debug(s"Stream failed with: $rootCause, going to restart", e)
      Supervision.Restart
    case _ => Supervision.Stop
  }

  val scaleFactor = 1 //Raise to widen range of IDs and thus have more traffic
  val evictionTime: FiniteDuration = 5.minutes //Lower eg to 5.seconds to see cache and file system deletes
  val evictionTimeOnError: FiniteDuration = 10.minutes
  val localFileCache: Path = Paths.get(System.getProperty("java.io.tmpdir")).resolve("localFileCache")

  logger.info(s"Starting with localFileCache dir: $localFileCache")
  FileUtils.forceMkdir(localFileCache.toFile)
  // Comment out to start with empty local file storage
  // Note that this may provoke "CACHE miss" cases, when we try to scavenge during recoverWith
  //FileUtils.cleanDirectory(localFileCache.toFile)


  val writer: CacheWriter[Int, Path] = new caffeine.cache.CacheWriter[Int, Path] {
    override def write(key: Int, value: Path): Unit = {
      logger.debug(s"TRACE_ID: $key write to cache")
    }

    override def delete(key: Int, value: Path, cause: caffeine.cache.RemovalCause): Unit = {
      logger.info(s"TRACE_ID: $key delete from file system because of: $cause")
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

  //Sync loading, because AsyncLoadingCache does not work with CacheWriter
  //see: https://stackoverflow.com/questions/48356731/caffeine-cant-provide-cachewriter-to-asyncloadingcache/48361538#48361538
  val loadedResults: util.List[Path] = new FileLister().run(localFileCache.toFile)
  loadedResults.forEach { path: Path =>
    val lastAccessTime = Files.readAttributes(path, classOf[BasicFileAttributes]).lastAccessTime
    logger.debug(s"Add file: ${path.toFile.getName} with lastAccessTime: $lastAccessTime}")
    cache.put(path.toFile.getName.dropRight(4).toInt, path)
  }

  def main(args: Array[String]): Unit = {
    case class Message(group: Int, id: Int, file: Path)

    val downloadFlow: Flow[Message, Message, NotUsed] = Flow[Message]
      .mapAsyncUnordered(5) { message =>
        def processNext(message: Message): Message = {
          val key = message.id
          val url = new URI("http://127.0.0.1:6001/downloadni/" + key.toString)
          val destinationFile = localFileCache.resolve(Paths.get(message.id.toString + ".zip"))

          //Using get(key => downloadfunction) is preferable to getIfPresent, because the get method performs the computation atomically
          val downloadedFile = cache.get(message.id, key => new DownloaderRetry().download(key, url, destinationFile))
          logger.info(s"TRACE_ID: ${message.id} successfully read from cache")
          Message(message.group, message.id, downloadedFile)
        }

        //If we get a 404 from the server, use this "optimistic approach" to retry
        def attemptToRecover(message: Message, e: RuntimeException) = {
          val rootCause = ExceptionUtils.getRootCause(e)
          val status = rootCause.asInstanceOf[HttpResponseException].getStatusCode
          val resultPromise = Promise[Message]()

          if (status == 404) {
            logger.info(s"TRACE_ID: ${message.id} Request failed with 404, wait for the file to appear in the local cache (from a concurrent download)")
            Thread.sleep(10000)
            if (cache.getIfPresent(message.id).isDefined) {
              val value = cache.getIfPresent(message.id).get
              logger.info(s"TRACE_ID: ${message.id} CACHE hit")
              resultPromise.success(Message(message.group, message.id, value))
            } else {
              logger.info(s"TRACE_ID: ${message.id} CACHE miss")
              resultPromise.failure(e)
            }
          }
          resultPromise.future
        }

        Future(processNext(message)).recoverWith {
          case e: RuntimeException if ExceptionUtils.getRootCause(e).isInstanceOf[HttpResponseException] => {
            attemptToRecover(message, e)
          }
          case e: Throwable => Future.failed(e)
        }
      }

    val faultyDownstreamFlow: Flow[Message, Message, NotUsed] = Flow[Message]
      .map { message: Message =>
        if (message.group == 0) {
          //Force an update (= replacement of value) to extend time in cache. evictionTimeOnError will be used
          logger.info(s"TRACE_ID: ${message.id} extend eviction time for message in group: ${message.group}")
          cache.put(message.id, message.file)

        }
        message
      }

    //Generate random messages with IDs between 0/50 note that after a while we will have only cache hits
    //Done like this to have a higher chance of producing messages with the same ID within the same time
    val messagesGroupZero = List.fill(10000)(Message(0, ThreadLocalRandom.current.nextInt(0, 50 * scaleFactor), null))
    val messagesGroupOne = List.fill(10000)(Message(1, ThreadLocalRandom.current.nextInt(0, 50 * scaleFactor), null))
    val messagesGroupTwo = List.fill(10000)(Message(2, ThreadLocalRandom.current.nextInt(0, 50 * scaleFactor), null))

    val combinedMessages = Source.combine(Source(messagesGroupZero), Source(messagesGroupOne), Source(messagesGroupTwo))(numInputs => MergePrioritized(List(1, 1, 1)))
    combinedMessages
      .throttle(2 * scaleFactor, 1.second, 2 * scaleFactor, ThrottleMode.shaping)
      .via(downloadFlow)
      //.via(faultyDownstreamFlow)
      .withAttributes(ActorAttributes.supervisionStrategy(deciderFlow))
      .runWith(Sink.ignore)
  }
}

package sample.stream_shared_state

import java.io.File
import java.net.URI
import java.nio.file.{Path, Paths}
import java.util
import java.util.concurrent.ThreadLocalRandom

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, MergePrioritized, Sink, Source}
import akka.stream.{ActorAttributes, ActorMaterializer, Supervision, ThrottleMode}
import com.github.benmanes.caffeine
import com.github.blemale.scaffeine.{Cache, Scaffeine}
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.http.client.HttpResponseException
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
      val rootCause = ExceptionUtils.getRootCause(e)
      logger.info(s"Stream failed with: $rootCause, going to restart")
      logger.debug(s"Stream failed with: $rootCause, going to restart", e)
      Supervision.Restart
    case _ => Supervision.Stop
  }

  val scaleFactor = 1 //Raise to stress more
  val evictionTime = 5.minutes
  val evictionTimeOnError = 10.minutes
  val localFileCache = Paths.get(System.getProperty("java.io.tmpdir")).resolve("localFileCache")

  FileUtils.forceMkdir(localFileCache.toFile)
  //Comment out to start with empty local file cache
  FileUtils.cleanDirectory(localFileCache.toFile)


  val writer = new caffeine.cache.CacheWriter[Int, Path] {
    override def write(key: Int, value: Path): Unit = {
      logger.debug(s"Writing to cache TRACE_ID: $key")
    }

    override def delete(key: Int, value: Path, cause: caffeine.cache.RemovalCause): Unit = {
      logger.info(s"Deleting from storage for TRACE_ID: $key because of: $cause")
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

    val downloadFlow: Flow[Message, Message, NotUsed] = Flow[Message]
      .mapAsyncUnordered(5) { message =>

        def processNext(message: Message): Message = {
          if (cache.getIfPresent(message.id).isDefined) {
            val value = cache.getIfPresent(message.id).get
            logger.info(s"Cache hit for TRACE_ID: ${message.id}")
            Message(message.group, message.id, value)
          } else {
            logger.info(s"Cache miss for TRACE_ID: ${message.id} - download...")
            var downloadedFile: Path = null

              val destinationFile = localFileCache.resolve(Paths.get(message.id.toString + ".zip"))
              //val url = new URI("http://127.0.0.1:6001/downloadflaky/" + message.id.toString)
              val url = new URI("http://127.0.0.1:6001/downloadni/" + message.id.toString)
              downloadedFile = new DownloaderRetry().download(url, destinationFile)
              logger.info(s"Successfully downloaded for TRACE_ID: ${message.id} - put into cache...")
              cache.put(message.id, downloadedFile)
              Message(message.group, message.id, downloadedFile)
          }
        }
        //TODO Find a way to wait for the downloaded element non-blocking
        //For now: Instead of blocking concurrent requests before the cache lookup
        //lets use this "optimistic" approach
        Future(processNext(message)).recoverWith {
          case e: RuntimeException if ExceptionUtils.getRootCause(e).isInstanceOf[HttpResponseException] =>  {
            val rootCause = ExceptionUtils.getRootCause(e)
            val status = rootCause.asInstanceOf[HttpResponseException].getStatusCode
            var returnFuture = Future(Message(0, 0, null))
            if (status == 404) {
                logger.info(s"TRACE_ID: ${message.id} failed with 404, wait for the item to appear in the local cache (from a concurrent download)")
                Thread.sleep(10000) //... but we block here on the current thread :-(
                if (cache.getIfPresent(message.id).isDefined) {
                  val value = cache.getIfPresent(message.id).get
                  logger.info(s"CACHE hit for TRACE_ID: ${message.id}")
                  returnFuture = Future(Message(message.group, message.id, value))
                } else {
                  logger.info(s"NO CACHE hit for TRACE_ID: ${message.id}")
                  returnFuture = Future.failed(e)
                }
              }
            returnFuture
          }
          case e: Throwable =>  Future.failed(e)
        }
      }

    val faultyDownstreamFlow: Flow[Message, Message, NotUsed] = Flow[Message]
      .map { message: Message =>
        try {
          if (message.group == 0) throw new RuntimeException(s"Simulate error for message: ${message.id} in group 0")
        } catch {
          case e@(_: RuntimeException) => {
            //Force an update (= replacement of value) to extend cache time
            logger.info(s"Extend eviction time for TRACE_ID: ${message.id} from group: ${message.group}")
            cache.put(message.id, message.file)
          }
        }
        message
      }

    //Within three groups: Generate random messages with IDs between 0/100, set to 5 to show "download barrier" issue
    val messagesGroupOne = List.fill(10000)(Message(1, ThreadLocalRandom.current.nextInt(0, 50 * scaleFactor), null))
    val messagesGroupTwo = List.fill(10000)(Message(2, ThreadLocalRandom.current.nextInt(0, 50 * scaleFactor), null))
    val messagesGroupThree = List.fill(10000)(Message(3, ThreadLocalRandom.current.nextInt(0, 50 * scaleFactor), null))

    val combinedMessages = Source.combine(Source(messagesGroupOne), Source(messagesGroupTwo), Source(messagesGroupThree))(numInputs => MergePrioritized(List(1,1,1)))
    combinedMessages
      .throttle(2 * scaleFactor, 1.second, 2 * scaleFactor, ThrottleMode.shaping)
      //Try to go parallel on the TRACE_ID and thus have two substreams
      .groupBy(2, _.id % 2)
      .via(downloadFlow)
//      .via(faultyDownstreamFlow)
      .mergeSubstreams
      .withAttributes(ActorAttributes.supervisionStrategy(deciderFlow))
      .runWith(Sink.ignore)
  }
}

package sample.loadbalancer.external

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling._
import akka.stream.alpakka.elasticsearch.WriteMessage.createIndexMessage
import akka.stream.alpakka.elasticsearch._
import akka.stream.alpakka.elasticsearch.scaladsl.{ElasticsearchSink, ElasticsearchSource}
import akka.stream.scaladsl.{FileIO, Sink, Source}
import akka.stream.{Supervision, ThrottleMode}
import org.slf4j.{Logger, LoggerFactory}
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.utility.DockerImageName
import spray.json.DefaultJsonProtocol

import java.io.File
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.sys.process.Process
import scala.util.control.NonFatal
import scala.util.{Failure, Success}


trait JsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {

  case class Ctx(fileName: String, ocrResponse: String)

  implicit def formatCtx = jsonFormat2(Ctx)

}


/**
  * Use OCR as a CPU intensive operation (done in external containers)
  * to show load balancing (done via external nginx conf as load balancer)
  *
  * Prerequisites:
  *
  * Clone:
  * git clone https://github.com/nassiesse/simple-ocr-microservice.git
  * Some tweaks needed (Create fork?)
  * build with:
  * mvn package
  * docker build -t nassiesse/simple-java-ocr .
  *
  *
  * Start all containers with:
  * docker-compose up -d  --build nginx
  *
  *
  * Needs patience because of:
  *  - external processing on docker
  *  - startup elasticsearch
  *
  * TODOs
  * - Bootstrap nginx from code via GenericContainer?
  * - Play with larger number of Docs - Buffers? backpressure?
  * - Switch off worker images manually -> Resilience via Pool?
  * - Worker images are crashing and hanging but PING is OK...
  * - Possible to add metainfo on which Worker the File was processed?
  *
  */
object LoadBalancerExternal extends App with JsonProtocol {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("LoadBalancerExternal")

  import system.dispatcher

  val decider: Supervision.Decider = {
    case NonFatal(e) =>
      logger.warn(s"Stream failed with: $e, going to restart")
      Supervision.Restart
  }

  val dockerImageName = DockerImageName
    .parse("docker.elastic.co/elasticsearch/elasticsearch-oss")
    .withTag("7.10.2")
  val elasticsearchContainer = new ElasticsearchContainer(dockerImageName)
  elasticsearchContainer.start()

  val address = elasticsearchContainer.getHttpHostAddress
  val connectionSettings = ElasticsearchConnectionSettings(s"http://$address")

  // This index will be created in Elasticsearch on the fly, when content is added
  val indexName = "searchresults"
  val elasticsearchParamsV7 = ElasticsearchParams.V7(indexName)

  val matchAllQuery = """{"match_all": {}}"""

  val sourceSettings = ElasticsearchSourceSettings(connectionSettings).withApiVersion(ApiVersion.V7)
  val elasticsearchSourceTyped = ElasticsearchSource
    .typed[Ctx](
      elasticsearchParamsV7,
      query = matchAllQuery,
      settings = sourceSettings
    )

  val sinkSettings =
    ElasticsearchWriteSettings(connectionSettings)
      .withBufferSize(10)
      .withVersionType("internal")
      .withRetryLogic(RetryAtFixedRate(maxRetries = 5, retryInterval = 1.second))
      .withApiVersion(ApiVersion.V7)
  val elasticsearchSink =
    ElasticsearchSink.create[Ctx](
      elasticsearchParamsV7,
      settings = sinkSettings
    )

  logger.info(s"Elasticsearch container listening on: ${elasticsearchContainer.getHttpHostAddress}")
  logger.info("About to start processing flow...")


  val (ocrProxyHost, ocrProxyPort) = ("127.0.0.1", 8081)
  val resourceFileName = "test_ocr.pdf"

  // Unbounded stream. Limited for testing purposes by appending eg .take(5)
  val filesToUpload = Source(LazyList.continually(new File(s"src/main/resources/$resourceFileName"))).take(6)

  val hostConnectionPoolUpload = Http().cachedHostConnectionPool[File](ocrProxyHost, ocrProxyPort)

  def createEntityFrom(file: File): Future[RequestEntity] = {
    require(file.exists())
    val fileSource = FileIO.fromPath(file.toPath, chunkSize = 1000000)
    val paramMapFile = Map("type" -> "application/octet-stream", "filename" -> file.getName)
    val formData = Multipart.FormData(Multipart.FormData.BodyPart(
      "file",
      HttpEntity(MediaTypes.`application/octet-stream`, file.length(), fileSource), paramMapFile))
    Marshal(formData).to[RequestEntity]
  }

  def createUploadRequest(fileToUpload: File): Future[(HttpRequest, File)] = {
    logger.info(s"About to create upload request for file: $fileToUpload")
    val target = Uri(s"http://$ocrProxyHost:$ocrProxyPort").withPath(akka.http.scaladsl.model.Uri.Path("/api/pdf/extractText"))

    createEntityFrom(fileToUpload)
      .map(entity => HttpRequest(HttpMethods.POST, uri = target, entity = entity))
      .map(each => (each, fileToUpload))
  }


  filesToUpload
    .throttle(1, 20.second, 1, ThrottleMode.shaping)
    .mapAsync(1)(createUploadRequest)
    .via(hostConnectionPoolUpload)
    .map {
      case (Success(HttpResponse(StatusCodes.OK, _, entity, _)), fileToUpload: File) =>
        logger.info(s"OCR processing for file: $fileToUpload was successful")

        // Converting directly does not work
        //val ctxFuture = Unmarshal(res.entity).to[Ctx]
        val jsonF = Unmarshal(entity).to[String]
        val json = Await.result(jsonF, 1.second)
        Ctx(fileToUpload.getName, json)
      case (Failure(ex), fileToUpload) =>
        throw new RuntimeException(s"OCR processing for file: $fileToUpload was NOT successful. Ex: $ex")
    }

    .wireTap(each => logger.info(s"Add to index: $each"))

    // TODO Better without, because of long processing times?
    //.withAttributes(ActorAttributes.supervisionStrategy(decider))
    .map(ctx => createIndexMessage(ctx))
    .runWith(elasticsearchSink)


  // Wait for the index to populate
  Thread.sleep(30.seconds.toMillis)
  browserClient()
  Source.tick(1.seconds, 10.seconds, ())
    .map(_ => query())
    .runWith(Sink.ignore)


  private def browserClient() = {
    val os = System.getProperty("os.name").toLowerCase
    val searchURL = s"http://localhost:${elasticsearchContainer.getMappedPort(9200)}/$indexName/_search?q=*"
    if (os == "mac os x") {
      Process(s"open $searchURL").!
    }
    else {
      logger.info(s"Please open a browser at: $searchURL")
    }
  }


  private def query() = {
    logger.info(s"About to execute read query...")
    for {
      result <- elasticsearchSourceTyped.runWith(Sink.seq)
    } {
      logger.info(s"Read typed: ${result.size}. 1st element: ${result.head}")
    }
  }
}

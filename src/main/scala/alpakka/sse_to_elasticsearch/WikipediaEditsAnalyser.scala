package alpakka.sse_to_elasticsearch

import dev.langchain4j.data.document.Document
import dev.langchain4j.data.document.splitter.DocumentSplitters
import dev.langchain4j.data.message.UserMessage
import dev.langchain4j.data.segment.TextSegment
import dev.langchain4j.memory.chat.MessageWindowChatMemory
import dev.langchain4j.model.chat.request.ResponseFormat
import dev.langchain4j.model.embedding.onnx.bgesmallenv15q.BgeSmallEnV15QuantizedEmbeddingModel
import dev.langchain4j.model.ollama.OllamaChatModel
import dev.langchain4j.model.openai.OpenAiChatModel
import dev.langchain4j.model.openai.OpenAiChatModelName.GPT_4_O_MINI
import dev.langchain4j.rag.DefaultRetrievalAugmentor
import dev.langchain4j.rag.content.retriever.EmbeddingStoreContentRetriever
import dev.langchain4j.rag.query.router.DefaultQueryRouter
import dev.langchain4j.service.AiServices
import dev.langchain4j.store.embedding.inmemory.InMemoryEmbeddingStore
import io.circe.*
import io.circe.generic.auto.*
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.parser.*
import io.circe.syntax.*
import layoutz.*
import opennlp.tools.namefind.{NameFinderME, TokenNameFinderModel}
import opennlp.tools.tokenize.{TokenizerME, TokenizerModel}
import opennlp.tools.util.Span
import org.apache.commons.lang3.StringUtils
import org.apache.commons.text.StringEscapeUtils
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.sse.ServerSentEvent
import org.apache.pekko.http.scaladsl.server.Directives.{as, complete, concat, entity, get, getFromFile, onComplete, path, pathEndOrSingleSlash, pathPrefix, post}
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.stream.connectors.elasticsearch.*
import org.apache.pekko.stream.connectors.elasticsearch.WriteMessage.createIndexMessage
import org.apache.pekko.stream.connectors.elasticsearch.scaladsl.{ElasticsearchSink, ElasticsearchSource}
import org.apache.pekko.stream.scaladsl.{Flow, RestartSource, Sink, Source}
import org.apache.pekko.stream.{ActorAttributes, RestartSettings, Supervision}
import org.opensearch.testcontainers.OpensearchContainer
import org.slf4j.{Logger, LoggerFactory}
import org.testcontainers.utility.DockerImageName
import spray.json.DefaultJsonProtocol.*
import spray.json.RootJsonFormat

import java.io.FileInputStream
import java.net.URLEncoder
import java.nio.file.Paths
import java.time.{Duration, Instant, ZoneId}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.sys.process.{Process, stringSeqToProcess}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/**
  * Consume Wikipedia edits via SSE (like in [[alpakka.sse.SSEClientWikipediaEdits]]),
  * fetch the extract via Wikipedia API,
  * do local and remote NER processing for persons in EN
  * and then write the results to an Opensearch version 2.x server
  * Also transform the content as embeddings to a local [[InMemoryEmbeddingStore]]
  * to be able to RAG chat with them via a local [[Assistant]]
  *
  * Remarks:
  *  - Local means: Requests on local machine (local LLM via Docker ollama or via Java nlp lib)
  *  - Remote means: Requests to a remote OpenAI LLM accessed with API-Key
  *  - We use [[spray.json]] because of the Elasticsearch pekko connector
  *
  * Doc:
  * https://pekko.apache.org/docs/pekko-connectors/current/elasticsearch.html
  * https://www.testcontainers.org/modules/elasticsearch
  * https://pekko.apache.org/docs/pekko-connectors/current/opensearch.html
  * https://github.com/opensearch-project/opensearch-testcontainers
  */
trait Assistant {
  def answer(query: String): String
}

object WikipediaEditsAnalyser extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  private val decider: Supervision.Decider = {
    case NonFatal(e) =>
      logger.warn(s"Stream failed with: $e, going to restart")
      Supervision.Restart
  }

  // Set to false for now, because local Ollama is still experimental
  private val useLocalOllamaNER: Boolean = false

  // Switch off at runtime via UI to save costs
  private val isRemoteProcessingEnabled = new AtomicBoolean(true)

  // 2.x model from https://opennlp.apache.org/models.html
  private val tokenModel = new TokenizerModel(new FileInputStream(Paths.get("src/main/resources/opennlp-en-ud-ewt-tokens-1.2-2.5.0.bin").toFile))
  // 1.5 model from https://opennlp.sourceforge.net/models-1.5
  private val personModel = new TokenNameFinderModel(new FileInputStream(Paths.get("src/main/resources/en-ner-person.bin").toFile))

  private val embeddingStore = new InMemoryEmbeddingStore[TextSegment]()
  private val embeddingModel = new BgeSmallEnV15QuantizedEmbeddingModel()
  private val contentRetriever = EmbeddingStoreContentRetriever.builder()
    .embeddingStore(embeddingStore)
    .embeddingModel(embeddingModel)
    .maxResults(2)
    .minScore(0.6)
    .build()
  val OPENAI_API_KEY = "***"

  case class Change(timestamp: Long, title: String, serverName: String, user: String, cmdType: String, isBot: Boolean, isNamedBot: Boolean, lengthNew: Int = 0, lengthOld: Int = 0) {
    def traceId: String = title.hashCode.abs.toString
  }

  object Change extends ((Long, String, String, String, String, Boolean, Boolean, Int, Int) => Change) {
    def apply(timestamp: Long, title: String, serverName: String, user: String, cmdType: String, isBot: Boolean, isNamedBot: Boolean, lengthNew: Int = 0, lengthOld: Int = 0): Change =
      new Change(timestamp, title, serverName, user, cmdType, isBot, isNamedBot, lengthNew, lengthOld)

    implicit def formatChange: RootJsonFormat[Change] = jsonFormat9(Change.apply)
  }

  // Helps to carry the data through the stages, although this violates functional principles
  case class Ctx(change: Change, personsFoundLocal: List[String] = List.empty, personsFoundRemote: List[String] = List.empty, content: String = "") {
    def traceId: String = change.traceId
  }

  private object Ctx extends ((Change, List[String], List[String], String) => Ctx) {
    def apply(change: Change, personsFoundLocal: List[String] = List.empty, personsFoundRemote: List[String] = List.empty, content: String = ""): Ctx =
      new Ctx(change, personsFoundLocal, personsFoundRemote, content)

    implicit def formatCtx: RootJsonFormat[Ctx] = jsonFormat4(Ctx.apply)
  }

  final case class Person(name: String)

  val ollamaContainer = new OllamaContainer()
  ollamaContainer.start()

  private val dockerImageNameOS = DockerImageName
    .parse("opensearchproject/opensearch")
    .withTag("2.19.3")
  private val searchContainer = new OpensearchContainer(dockerImageNameOS)
  searchContainer.start()

  val address = searchContainer.getHttpHostAddress
  val connectionSettings = OpensearchConnectionSettings(s"$address")
    .withCredentials("user", "password")

  // For simplicity: This index will be created in Opensearch on the fly by the first entry
  private val indexName = "wikipediaedits"
  private val searchParams = OpensearchParams.V1(indexName)
  private val matchAllQuery = """{"match_all": {}}"""

  private val sourceSettings = ElasticsearchSourceSettings(connectionSettings).withApiVersion(ApiVersion.V7)

  // ElasticsearchSource reads are "scroll requests". Allows to fetch the entire collection of documents
  private val elasticsearchSourceTyped = ElasticsearchSource
    .typed[Ctx](
      searchParams,
      query = matchAllQuery,
      settings = sourceSettings
    )
  private val elasticsearchSourceRaw = ElasticsearchSource
    .create(
      searchParams,
      query = matchAllQuery,
      settings = sourceSettings
    )

  private val sinkSettings =
    ElasticsearchWriteSettings(connectionSettings)
      .withBufferSize(10)
      .withVersionType("internal")
      .withRetryLogic(RetryAtFixedRate(maxRetries = 5, retryInterval = 1.second))
      .withApiVersion(ApiVersion.V7)
  private val elasticsearchSink =
    ElasticsearchSink.create[Ctx](
      searchParams,
      settings = sinkSettings
    )


  import org.apache.pekko.http.scaladsl.unmarshalling.sse.EventStreamUnmarshalling.*

  val restartSettings = RestartSettings(1.second, 10.seconds, 0.2).withMaxRestarts(10, 1.minute)
  val restartSource = RestartSource.withBackoff(restartSettings) { () =>
    Source.futureSource {
      Http()
        .singleRequest(HttpRequest(
          uri = "https://stream.wikimedia.org/v2/stream/recentchange"
        ))
        .flatMap(Unmarshal(_).to[Source[ServerSentEvent, NotUsed]])
    }.withAttributes(ActorAttributes.supervisionStrategy(decider))
  }

  val parserFlow: Flow[ServerSentEvent, Change, NotUsed] = Flow[ServerSentEvent].map {
    serverSentEvent => {

      def isNamedBot(bot: Boolean, user: String): Boolean = {
        if (bot) user.toLowerCase().contains("bot") else false
      }

      val cursor = parse(serverSentEvent.data).getOrElse(Json.Null).hcursor

      val titleAsID = cursor.get[String]("title").toOption.getOrElse("")
      val timestamp: Long = cursor.get[Long]("timestamp").toOption.getOrElse(0)
      val serverName = cursor.get[String]("server_name").toOption.getOrElse("")
      val user = cursor.get[String]("user").toOption.getOrElse("")
      val cmdType = cursor.get[String]("type").toOption.getOrElse("")
      val bot = cursor.get[Boolean]("bot").toOption.getOrElse(false)

      if (cmdType == "new" || cmdType == "edit") {
        val length = cursor.downField("length")
        val lengthNew = length.get[Int]("new").toOption.getOrElse(0)
        val lengthOld = length.get[Int]("old").toOption.getOrElse(0)
        Change(timestamp, titleAsID, serverName, user, cmdType, isBot = bot, isNamedBot = isNamedBot(bot, user), lengthNew, lengthOld)
      } else {
        Change(timestamp, titleAsID, serverName, user, cmdType, isBot = bot, isNamedBot = isNamedBot(bot, user))
      }
    }
  }

  // Case classes to represent the Wikipedia API response structure
  case class WikipediaPage(
                            pageid: Option[Long],
                            ns: Option[Int],
                            title: Option[String],
                            extract: Option[String]
                          )

  case class WikipediaQuery(
                             pages: Map[String, WikipediaPage]
                           )

  case class WikipediaApiResponse(
                                   batchcomplete: Option[String],
                                   query: Option[WikipediaQuery]
                                 )

  // Circe decoders for Wikipedia API response
  implicit val wikipediaPageDecoder: Decoder[WikipediaPage] =
    Decoder.forProduct4("pageid", "ns", "title", "extract")(WikipediaPage.apply)

  implicit val wikipediaQueryDecoder: Decoder[WikipediaQuery] =
    Decoder.forProduct1("pages")(WikipediaQuery.apply)

  implicit val wikipediaApiResponseDecoder: Decoder[WikipediaApiResponse] =
    Decoder.forProduct2("batchcomplete", "query")(WikipediaApiResponse.apply)

  private def fetchContent(ctx: Ctx): Future[Ctx] = {
    logger.info(s"[${ctx.traceId}] About to read `extract` from Wikipedia entry with title: ${ctx.change.title}")
    val encodedTitle = URLEncoder.encode(ctx.change.title, "UTF-8")

    val requestURL = s"https://en.wikipedia.org/w/api.php?format=json&action=query&prop=extracts&exlimit=max&explaintext&exintro&titles=$encodedTitle"

    Http().singleRequest(HttpRequest(uri = requestURL))
      .flatMap(_.entity.toStrict(2.seconds))
      .map(_.data.utf8String)
      .map { jsonString =>
        logger.debug(s"[${ctx.traceId}] Raw Wikipedia API response: $jsonString")
        parse(jsonString) match {
          case Right(json) =>
            json.as[WikipediaApiResponse] match {
              case Right(apiResponse) =>
                val extractOpt = for {
                  query <- apiResponse.query
                  // Get the first page from the pages map (there should only be one for a single title request)
                  (_, page) <- query.pages.headOption
                  extract <- page.extract
                } yield extract

                extractOpt match {
                  case Some(extract) =>
                    logger.info(s"[${ctx.traceId}] Successfully extracted content: ${extract.take(100)}...")
                    ctx.copy(content = extract)
                  case None =>
                    logger.warn(s"[${ctx.traceId}] No extract found for title: ${ctx.change.title}")
                    ctx.copy(content = "")
                }

              case Left(decodingError) =>
                logger.error(s"[${ctx.traceId}] Failed to decode Wikipedia API response: $decodingError")
                ctx.copy(content = "")
            }
          case Left(parsingError) =>
            logger.error(s"[${ctx.traceId}] Failed to parse Wikipedia API JSON: $parsingError")
            ctx.copy(content = "")
        }
      }
      .recover {
        case ex: Exception =>
          logger.error(s"[${ctx.traceId}] Error fetching content from Wikipedia API: ${ex.getMessage}", ex)
          ctx.copy(content = "")
      }
  }

  private def findPersons(ctx: Ctx): Future[Ctx] = {
    val localNERFuture = if (useLocalOllamaNER) {
      findPersonsLocalOllamaNER(ctx)
    } else {
      findPersonsLocalNER(ctx)
    }
    localNERFuture.flatMap(localResult => findPersonsRemoteNER(localResult))
  }

  private def logNERResults(ctx: Ctx): Ctx = {
    val localPersons = ctx.personsFoundLocal
    val remotePersons = ctx.personsFoundRemote
    val allPersons = (localPersons ++ remotePersons).distinct

    if (allPersons.nonEmpty) {
      val localOnly = localPersons.diff(remotePersons)
      val remoteOnly = remotePersons.diff(localPersons)
      val both = localPersons.intersect(remotePersons)

      val title = s"NER results for '${ctx.change.title}'"
      val headers = Array("Category", "Count", "Names")
      val rowsListToDisplay = scala.collection.mutable.ListBuffer[Array[String]]()

      rowsListToDisplay += Array("Total Persons", allPersons.size.toString,
        if (allPersons.nonEmpty) allPersons.mkString(", ") else "none")
      if (localOnly.nonEmpty) {
        val nerTypeLabel = if (useLocalOllamaNER) "Local Ollama NER only" else "Local Java NLP NER only"
        rowsListToDisplay += Array(nerTypeLabel, localOnly.size.toString, localOnly.mkString(", "))
      }
      if (remoteOnly.nonEmpty) {
        rowsListToDisplay += Array("Remote NER only", remoteOnly.size.toString, remoteOnly.mkString(", "))
      }
      if (both.nonEmpty) {
        val bothLabel = if (useLocalOllamaNER) "Found by Ollama & Remote" else "Found by Java NLP & Remote"
        rowsListToDisplay += Array(bothLabel, both.size.toString, both.mkString(", "))
      }
      val table = formatAsAsciiTable(title, headers, rowsListToDisplay.toArray)
      logger.info(s"[${ctx.traceId}]\n$table")

      ctx
    } else {
      ctx
    }
  }

  private def sanitizePersonNames(names: List[String]): List[String] = {
    names
      .map(each => StringEscapeUtils.unescapeJava(each))
      // Keep name related content (letters, whitespace, apostrophes, periods, hyphens)
      .map(_.replaceAll("[^\\p{L}\\s'.\\-]", ""))
      .map(StringUtils.trim)
      .filter(StringUtils.isNotBlank)
  }

  private def findPersonsLocalNER(ctx: Ctx): Future[Ctx] = {
    logger.info(s"[${ctx.traceId}] Local Java NLP NER: About to find person names in: ${ctx.change.title}")
    val content = ctx.content

    // We need a new instance, because TokenizerME is not thread safe
    // Doc: https://opennlp.apache.org/docs/2.0.0/manual/opennlp.html
    // Chapter: Name Finder API
    val tokenizer = new TokenizerME(tokenModel)
    val tokens = tokenizer.tokenize(content)

    val personNameFinderME = new NameFinderME(personModel)
    val spans = personNameFinderME.find(tokens)
    val personsFound = Span.spansToStrings(NameFinderME.dropOverlappingSpans(spans), tokens).toList.distinct
    personNameFinderME.clearAdaptiveData()

    if (personsFound.isEmpty) {
      Future(ctx)
    } else {
      val personsFoundCleaned = sanitizePersonNames(personsFound)

      logger.info(s"[${ctx.traceId}] Local Java NLP NER found persons: $personsFoundCleaned from content: $content")
      Future(ctx.copy(personsFoundLocal = personsFoundCleaned))
    }
  }

  private def findPersonsRemoteNER(ctx: Ctx): Future[Ctx] = {
    if (!isRemoteProcessingEnabled.get()) {
      logger.debug(s"[${ctx.traceId}] Remote processing disabled - skipping remote NER")
      return Future(ctx)
    }

    logger.info(s"[${ctx.traceId}] Remote NER: About to find person names in: ${ctx.change.title}")
    val content = ctx.content

    if (content.isEmpty) {
      return Future(ctx)
    }

    val model = OpenAiChatModel.builder()
      .apiKey(OPENAI_API_KEY)
      .modelName(GPT_4_O_MINI)
      .temperature(0)
      .timeout(Duration.ofSeconds(30))
      .build()

    val promptPersons =
      """You are an expert Named Entity Recognition (NER) system specialized in identifying person names.
        |
        |Task: Extract all person names from the provided text. Follow these rules strictly:
        |
        |INCLUDE:
        |- Full names of real people (e.g., "John Smith", "Marie Curie")
        |- Single names when clearly referring to people (e.g., "Einstein", "Shakespeare")
        |- Historical figures and public personalities
        |- Names with titles when referring to people (e.g., "Dr. Johnson", "President Lincoln")
        |
        |EXCLUDE:
        |- Organizations, companies, institutions
        |- Places, cities, countries, geographical locations
        |- Products, brands, software names
        |- Book titles, movie titles, song titles
        |- Abstract concepts or general terms
        |
        |OUTPUT FORMAT:
        |- Return each person name on a separate line
        |- Use the exact form as it appears in the text
        |- If no person names are found, return exactly: "NONE"
        |- Do not include explanations or additional text
        |
        |TEXT TO ANALYZE:
        |{{content}}
        |
        |PERSON NAMES:""".stripMargin

    val message = UserMessage.from(promptPersons.replace("{{content}}", content))

    try {
      val response = model.chat(message)
      val personsFoundText = response.aiMessage().text().trim()

      val personsFoundList = if (personsFoundText.isEmpty || personsFoundText.equalsIgnoreCase("NONE")) {
        List.empty[String]
      } else {
        val rawNames = personsFoundText.split("\n")
          .map(_.trim)
          .filter(_.nonEmpty)
          .filter(!_.equalsIgnoreCase("NONE"))
          .toList
        sanitizePersonNames(rawNames)
      }

      if (personsFoundList.isEmpty) {
        Future(ctx)
      } else {
        logger.info(s"[${ctx.traceId}] Remote NER found persons: $personsFoundList from content: $content")
        Future(ctx.copy(personsFoundRemote = personsFoundList))
      }
    } catch {
      case e: Exception =>
        logger.error(s"[${ctx.traceId}] Error during remote LLM call: ${e.getMessage}", e)
        Future(ctx)
    }
  }

  private def findPersonsLocalOllamaNER(ctx: Ctx): Future[Ctx] = {
    logger.info(s"[${ctx.traceId}] Local Ollama NER: About to find person names in: ${ctx.change.title}")
    val content = ctx.content

    if (content.isEmpty) {
      return Future(ctx)
    }

    val model = OllamaChatModel.builder
      .baseUrl(ollamaContainer.getBaseUrl)
      .modelName("llama3.2:1b")
      .temperature(0)
      .topP(0.1)
      .responseFormat(ResponseFormat.JSON)
      .timeout(Duration.ofSeconds(30))
      .build()

    val promptPersons =
      """You are a precise name extraction tool. Extract ONLY actual person names that are LITERALLY PRESENT in this text:
        |{{content}}
        |
        |Rules:
        |- Do NOT extract places: Geographical locations including countries, cities, regions, landmarks, or specific addresses
        |- Do NOT extract organizations: Names of companies, institutions, government bodies, or any other formal groups
        |- If unsure whether something is a person name, exclude it
        |- Return output as JSON with exactly this structure: {"names": ["name1", "name2", ...]}
        |- If no persons are found in text, return exactly: {"names": []}
        |
        |Examples:
        |Text: "John Smith visited the library yesterday."
        |Response: {"names": ["John Smith"]}
        |
        |Text: "The weather is sunny today in California."
        |Response: {"names": []}
        |
        |Text: "This category is for articles with short descriptions defined on Wikipedia by {{short description}}"
        |Response: {"names": []}
        |
        """.stripMargin

    val message = UserMessage.from(promptPersons.replace("{{content}}", content))

    try {
      val response = model.chat(message)
      val personsFoundText = response.aiMessage().text().trim()
      val personsFoundList = parseJSONResponse(personsFoundText)

      if (personsFoundList.isEmpty) {
        Future(ctx)
      } else {
        val verifiedPersons = personsFoundList.filter { personName =>
          val isPresent = content.toLowerCase.contains(personName.toLowerCase)
          if (!isPresent) {
            logger.debug(s"[${ctx.traceId}] Skipping: $personName - not found in content")
          }
          isPresent
        }

        if (verifiedPersons.isEmpty) {
          logger.debug(s"[${ctx.traceId}] No verified persons found after content validation")
          Future(ctx)
        } else {
          logger.info(s"[${ctx.traceId}] Local Ollama NER found persons: $verifiedPersons from content: $content")
          Future(ctx.copy(personsFoundLocal = verifiedPersons))
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s"[${ctx.traceId}] Error during local Ollama LLM call: ${e.getMessage}", e)
        Future(ctx)
    }
  }

  private def parseJSONResponse(personsFoundText: String) = {
    val personsFoundList = if (personsFoundText.isEmpty) {
      List.empty[String]
    } else {
      import io.circe.parser.*
      parse(personsFoundText) match {
        case Right(json) =>
          json.hcursor.downField("names").as[List[String]] match {
            case Right(names) => sanitizePersonNames(names)
            case Left(_) =>
              List.empty[String]
          }
        case Left(_) =>
          List.empty[String]
      }
    }
    personsFoundList
  }

  /**
    * Formats data as an ASCII table using the layoutz library.
    *
    * @param title   The title to display at the top of the table
    * @param headers Column headers
    * @param data    Table data as rows of columns
    * @return A formatted ASCII table as a string using layoutz
    */
  private def formatAsAsciiTable(title: String, headers: Array[String], data: Array[Array[String]]): String = {
    layout(
      section(title)(
        table(
          headers = headers.toSeq,
          rows = data.toSeq.map(_.toSeq)
        )
      )
    ).render
  }

  private val nerProcessingFlow: Flow[Change, Ctx, NotUsed] = Flow[Change]
    .filter(change => !change.isBot)
    .map(change => Ctx(change))
    .mapAsync(3)(ctx => fetchContent(ctx))
    .mapAsync(3)(ctx => findPersons(ctx))
    .map(ctx => logNERResults(ctx))

  private val embeddingStoreSink = Flow[Ctx]
    .filter(ctx => ctx.content.nonEmpty)
    .map(ctx => addToEmbeddingStore(ctx.content))
    .to(Sink.ignore)


  logger.info(s"Opensearch container listening on: ${searchContainer.getHttpHostAddress}")
  logger.info(s"NER Configuration - Using Local Ollama NER: $useLocalOllamaNER")
  if (useLocalOllamaNER) {
    logger.info(s"Local Ollama container base URL: ${ollamaContainer.getBaseUrl}")
  } else {
    logger.info("Using local Java NLP models for NER extraction")
  }
  logger.info("About to start processing flow...")

  restartSource
    .via(parserFlow)
    .via(nerProcessingFlow)
    .alsoTo(embeddingStoreSink)
    .map(ctx => createIndexMessage(dateTimeFormatted(ctx.change.timestamp), ctx))
    .wireTap(each => logger.debug(s"Add to index: $each"))
    .withAttributes(ActorAttributes.supervisionStrategy(decider))
    .runWith(elasticsearchSink)

  // Wait for the index "wikipediaedits" to populate
  Thread.sleep(20.seconds.toMillis)
  aiClient()

  Source.tick(1.seconds, 10.seconds, ())
    .map(_ => query())
    .runWith(Sink.ignore)


  private def aiClient(): Unit = {
    val assistant = createAssistant()
    startConversationWith(assistant)
  }

  private def createAssistant() = {
    val queryRouter = new DefaultQueryRouter(contentRetriever)
    val retrievalAugmentor = DefaultRetrievalAugmentor.builder.queryRouter(queryRouter).build
    val model = OpenAiChatModel.builder.apiKey(OPENAI_API_KEY).modelName(GPT_4_O_MINI).build

    AiServices
      .builder(classOf[Assistant])
      .chatModel(model)
      .retrievalAugmentor(retrievalAugmentor)
      .chatMemory(MessageWindowChatMemory
        .withMaxMessages(10)).build
  }

  // Case classes for Circe JSON parsing
  case class ElasticsearchCountResponse(count: Long, _shards: ShardsInfo)

  case class ShardsInfo(total: Int, successful: Int, skipped: Int, failed: Int)

  case class IndexCountResponse(count: Long)

  // Circe decoders
  implicit val shardsInfoDecoder: Decoder[ShardsInfo] = deriveDecoder[ShardsInfo]
  implicit val elasticsearchCountResponseDecoder: Decoder[ElasticsearchCountResponse] = deriveDecoder[ElasticsearchCountResponse]
  implicit val indexCountResponseEncoder: Encoder[IndexCountResponse] = deriveEncoder[IndexCountResponse]

  private def getIndexCount(): Future[IndexCountResponse] = {
    val urlCount = s"http://localhost:${searchContainer.getMappedPort(9200)}/$indexName/_count"

    Http().singleRequest(HttpRequest(uri = urlCount))
      .flatMap { response =>
        response.status match {
          case StatusCodes.OK =>
            Unmarshal(response.entity).to[String].flatMap { jsonString =>
              decode[ElasticsearchCountResponse](jsonString) match {
                case Right(esResponse) =>
                  Future.successful(IndexCountResponse(esResponse.count))
                case Left(error) =>
                  Future.failed(new RuntimeException(s"Failed to parse Opensearch response: $error"))
              }
            }
          case _ =>
            response.discardEntityBytes()
            Future.failed(new RuntimeException(s"Opensearch request failed with status: ${response.status}"))
        }
      }
      .recover {
        case ex: Exception =>
          logger.error(s"Error fetching index count: ${ex.getMessage}")
          IndexCountResponse(0) // Return 0 on error
      }
  }


  // Doc:
  // https://pekko.apache.org/docs/pekko-connectors/current/opensearch.html
  // https://docs.opensearch.org/docs/latest/query-dsl/full-text/simple-query-string/#simple-query-string-syntax
  private def searchPersons(query: String): Future[List[Person]] = {
    logger.info(s"Searching for persons with query: $query")

    val wildcard = "**"
    val searchQuery = if (query.equals(wildcard)) {
      """{
        "bool": {
          "should": [
            {
              "exists": {
                "field": "personsFoundLocal"
              }
            },
            {
              "exists": {
                "field": "personsFoundRemote"
              }
            }
          ],
          "minimum_should_match": 1
        }
    }"""
    } else {
      s"""{
        "simple_query_string": {
          "fields": [ "personsFoundLocal", "personsFoundRemote" ],
          "query": "$query*"
        }
    }"""
    }

    val searchSource = ElasticsearchSource
      .typed[Ctx](
        searchParams,
        query = searchQuery,
        settings = sourceSettings
      )

    searchSource
      .runWith(Sink.seq)
      .map { results =>
        val allPersonsFound = results.flatMap { readResult =>
          val localPersons = readResult.source.personsFoundLocal
          val remotePersons = readResult.source.personsFoundRemote
          (localPersons ++ remotePersons).sorted
        }.distinct
        allPersonsFound.map(Person(_)).toList

      }
      .recover {
        case ex =>
          logger.error(s"Error searching persons with query: $query", ex)
          List.empty[Person]
      }
  }

  private final case class QueryRequest(query: String)

  private final case class QueryResponse(answer: String)

  private final case class PersonSearchRequest(query: String)

  private final case class PersonSearchResponse(persons: List[Person])

  private final case class ProcessingControlRequest(enabled: Boolean)

  private final case class ProcessingControlResponse(enabled: Boolean, message: String)

  private final case class SearchIndexUrlResponse(url: String)

  private def startConversationWith(assistant: Assistant): Unit = {
    def enableProcessing(): ProcessingControlResponse = {
      if (!isRemoteProcessingEnabled.get()) {
        isRemoteProcessingEnabled.set(true)
        logger.info("Remote processing enabled - resuming remote LLM calls and indexing")
        ProcessingControlResponse(enabled = true, "Remote processing enabled - resuming remote LLM calls and indexing")
      } else {
        ProcessingControlResponse(enabled = true, "Remote processing already enabled")
      }
    }

    def disableProcessing(): ProcessingControlResponse = {
      if (isRemoteProcessingEnabled.get()) {
        isRemoteProcessingEnabled.set(false)
        val msg = "Remote processing disabled - suspending remote LLM calls and indexing (local NER continues)"
        logger.info(msg)
        ProcessingControlResponse(enabled = false, msg)
      } else {
        ProcessingControlResponse(enabled = false, "Remote processing already disabled")
      }
    }

    val route: Route =
      pathPrefix("assistant") {
        concat(
          path("indexCount") {
            get {
              onComplete(getIndexCount()) {
                case Success(countResponse) =>
                  complete(HttpEntity(ContentTypes.`application/json`, countResponse.asJson.noSpaces))
                case Failure(ex) =>
                  logger.error(s"Failed to get index count: ${ex.getMessage}")
                  complete(StatusCodes.InternalServerError -> s"""{"error": "Failed to get index count: ${ex.getMessage}"}""")
              }
            }
          },

          path("personsSearch") {
            post {
              entity(as[String]) { jsonString =>
                decode[PersonSearchRequest](jsonString) match {
                  case Right(request) =>
                    complete {
                      searchPersons(request.query).map { persons =>
                        PersonSearchResponse(persons: List[Person])
                      }.map(response =>
                        HttpEntity(ContentTypes.`application/json`, response.asJson.noSpaces)
                      ).recover {
                        case ex =>
                          logger.error("Error searching persons: ", ex)
                          HttpEntity(ContentTypes.`application/json`,
                            PersonSearchResponse(List.empty).asJson.noSpaces)
                      }
                    }
                  case Left(error) =>
                    complete(HttpResponse(400, entity = s"Invalid JSON: $error"))
                }
              }
            }
          },
          path("query") {
            post {
              entity(as[String]) { jsonString =>
                decode[QueryRequest](jsonString) match {
                  case Right(queryRequest) =>
                    val answer = assistant.answer(queryRequest.query)
                    val response = QueryResponse(answer)
                    complete(HttpEntity(ContentTypes.`application/json`, response.asJson.noSpaces))
                  case Left(error) =>
                    complete(HttpResponse(400, entity = s"Invalid JSON: $error"))
                }
              }
            }
          },
          path("control") {
            concat(
              post {
                entity(as[String]) { jsonString =>
                  decode[ProcessingControlRequest](jsonString) match {
                    case Right(controlRequest) =>
                      val response = if (controlRequest.enabled) enableProcessing() else disableProcessing()
                      complete(HttpEntity(ContentTypes.`application/json`, response.asJson.noSpaces))
                    case Left(error) =>
                      complete(HttpResponse(400, entity = s"Invalid JSON: $error"))
                  }
                }
              },
              get {
                val response = ProcessingControlResponse(isRemoteProcessingEnabled.get(),
                  if (isRemoteProcessingEnabled.get()) "Remote processing enabled" else "Remote processing disabled (only local NER active)")
                complete(HttpEntity(ContentTypes.`application/json`, response.asJson.noSpaces))
              }
            )
          },
          path("searchIndexUrl") {
            get {
              val url = s"http://localhost:${searchContainer.getMappedPort(9200)}/$indexName/_search?q=personsFoundLocal:*&size=100&pretty=true"
              val response = SearchIndexUrlResponse(url)
              complete(HttpEntity(ContentTypes.`application/json`, response.asJson.noSpaces))
            }
          },
          pathEndOrSingleSlash {
            get {
              val assistantHtml = Paths.get("src/main/resources/assistant.html").toFile
              getFromFile(assistantHtml, ContentTypes.`text/html(UTF-8)`)
            }
          }
        )
      }

    Http().newServerAt("localhost", 8080).bind(route)

    logger.info(s"AI assistant web interface listening at http://localhost:8080/assistant")

    val os = System.getProperty("os.name").toLowerCase
    val url = s"http://localhost:8080/assistant"
    if (os == "mac os x") Process(s"open $url").!
    else if (os.startsWith("windows")) Seq("cmd", "/c", s"start $url").!
    else logger.info(s"Please open a browser at: $url")
  }

  private def dateTimeFormatted(timestamp: Long) = {
    Instant.ofEpochSecond(timestamp).atZone(ZoneId.systemDefault).toLocalDateTime.toString
  }

  // Note that the size of the collection can also be fetched via a GET request, e.g.
  // http://localhost:57321/wikipediaedits/_count
  private def query(): Unit = {
    logger.info(s"About to execute scrolled read queries...")
    for {
      result <- readFromElasticsearchTyped()
      resultRaw <- readFromElasticsearchRaw()
    } {
      logger.info(s"Read typed: ${result.size}. 1st element: ${result.head}")
      logger.info(s"Read raw: ${resultRaw.size}. 1st element: ${resultRaw.head}")
    }
  }

  private def readFromElasticsearchTyped() = {
    elasticsearchSourceTyped.runWith(Sink.seq)
  }

  private def readFromElasticsearchRaw() = {
    elasticsearchSourceRaw.runWith(Sink.seq)
  }

  private def addToEmbeddingStore(text: String) = {
    val document = Document.from(text)
    val splitter = DocumentSplitters.recursive(300, 0)
    val segments = splitter.split(document)
    val embeddings = embeddingModel.embedAll(segments).content()
    embeddingStore.addAll(embeddings, segments)
  }
}
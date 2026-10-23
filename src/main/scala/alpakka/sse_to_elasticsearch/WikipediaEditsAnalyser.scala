package alpakka.sse_to_elasticsearch

import dev.langchain4j.data.document.Document
import dev.langchain4j.data.document.splitter.DocumentSplitters
import dev.langchain4j.data.message.{SystemMessage, UserMessage}
import dev.langchain4j.data.segment.TextSegment
import dev.langchain4j.memory.chat.MessageWindowChatMemory
import dev.langchain4j.model.embedding.onnx.bgesmallenv15q.BgeSmallEnV15QuantizedEmbeddingModel
import dev.langchain4j.model.openai.OpenAiChatModel
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
import org.apache.pekko.http.scaladsl.model.headers.`User-Agent`
import org.apache.pekko.http.scaladsl.model.sse.ServerSentEvent
import org.apache.pekko.http.scaladsl.server.Directives.{as, complete, concat, entity, get, getFromFile, onComplete, path, pathEndOrSingleSlash, pathPrefix, post}
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.stream.*
import org.apache.pekko.stream.connectors.elasticsearch.*
import org.apache.pekko.stream.connectors.elasticsearch.WriteMessage.createIndexMessage
import org.apache.pekko.stream.connectors.elasticsearch.scaladsl.{ElasticsearchSink, ElasticsearchSource}
import org.apache.pekko.stream.scaladsl.{Flow, RestartSource, Sink, Source}
import org.opensearch.testcontainers.OpensearchContainer
import org.slf4j.{Logger, LoggerFactory}
import org.testcontainers.utility.DockerImageName
import spray.json.DefaultJsonProtocol.*
import spray.json.RootJsonFormat

import java.io.FileInputStream
import java.net.URLEncoder
import java.nio.file.Paths
import java.time.Duration
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
  * Also transform the content and title as embeddings to a local [[InMemoryEmbeddingStore]]
  * to be able to RAG chat against it via [[Assistant]]
  *
  * Remarks:
  *  - Local NER means: NER processing on local machine (GLiNER service, with Java NLP as fallback)
  *  - Remote NER means: NER processing via remote OpenAI LLM
  *  - We use [[spray.json]] because of the Elasticsearch pekko connector
  *  - Env var: OPENAI_API_KEY
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

object WikipediaEditsAnalyser {
  def main(args: Array[String]): Unit = {
    new Application();
    ()
  }

  private class Application {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    implicit val system: ActorSystem = ActorSystem()

    import system.dispatcher

    private val decider: Supervision.Decider = {
      case NonFatal(e) =>
        logger.warn(s"Stream failed with: $e, going to restart")
        Supervision.Restart
    }

    // Default local NER. Prerequisite: Start service gliner-ner in docker-compose.yml
    // Local Java NLP NER is used as fallback, when the GLiNER service fails
    private val glinerServiceUrl = "http://localhost:8085/extract-persons"

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
    private val openAiApiKey: String = sys.env.getOrElse("OPENAI_API_KEY", "")
    private val openAiModelName = "gpt-6-luna"
    private val openAiReasoningEffort = "none"
    if (openAiApiKey.isEmpty) {
      throw new IllegalStateException("OPENAI_API_KEY environment variable not set!")
    }

    case class Change(timestamp: Long, title: String, wiki: String, serverName: String, user: String, cmdType: String, isBot: Boolean, isNamedBot: Boolean, lengthNew: Int = 0, lengthOld: Int = 0) {
      def traceId: String = title.hashCode.abs.toString
    }

    object Change {
      implicit val formatChange: RootJsonFormat[Change] = jsonFormat10(Change.apply)
    }

    // Helps to carry the data through the stages, although this violates functional principles
    case class Ctx(change: Change, personsFoundLocal: List[String] = List.empty, personsFoundRemote: List[String] = List.empty, content: String = "") {
      def traceId: String = change.traceId
    }

    private object Ctx {
      implicit val formatCtx: RootJsonFormat[Ctx] = jsonFormat4(Ctx.apply)
    }

    final case class Person(name: String)

    // GLiNER API models
    case class GLiNERExtractionRequest(text: String, threshold: Double = 0.5)

    case class PersonEntity(text: String, start: Int, end: Int, score: Double)

    case class GLiNERExtractionResponse(persons: List[PersonEntity], processing_time_ms: Double, text_length: Int)

    // Circe encoders/decoders for GLiNER models
    implicit val personEntityDecoder: Decoder[PersonEntity] = deriveDecoder[PersonEntity]
    implicit val glinerResponseDecoder: Decoder[GLiNERExtractionResponse] = deriveDecoder[GLiNERExtractionResponse]
    implicit val glinerRequestEncoder: Encoder[GLiNERExtractionRequest] = deriveEncoder[GLiNERExtractionRequest]

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
        val wiki = cursor.get[String]("wiki").toOption.getOrElse("")
        val serverName = cursor.get[String]("server_name").toOption.getOrElse("")
        val user = cursor.get[String]("user").toOption.getOrElse("")
        val cmdType = cursor.get[String]("type").toOption.getOrElse("")
        val bot = cursor.get[Boolean]("bot").toOption.getOrElse(false)

        if (cmdType == "new" || cmdType == "edit") {
          val length = cursor.downField("length")
          val lengthNew = length.get[Int]("new").toOption.getOrElse(0)
          val lengthOld = length.get[Int]("old").toOption.getOrElse(0)
          Change(timestamp, titleAsID, wiki, serverName, user, cmdType, isBot = bot, isNamedBot = isNamedBot(bot, user), lengthNew, lengthOld)
        } else {
          Change(timestamp, titleAsID, wiki, serverName, user, cmdType, isBot = bot, isNamedBot = isNamedBot(bot, user))
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
    implicit val wikipediaPageDecoder: Decoder[WikipediaPage] = deriveDecoder[WikipediaPage]
    implicit val wikipediaQueryDecoder: Decoder[WikipediaQuery] = deriveDecoder[WikipediaQuery]
    implicit val wikipediaApiResponseDecoder: Decoder[WikipediaApiResponse] = deriveDecoder[WikipediaApiResponse]

    private def fetchContent(ctx: Ctx): Future[Ctx] = {
      logger.info(s"[${ctx.traceId}] About to read `extract` from Wikipedia entry with title: ${ctx.change.title}")
      val encodedTitle = URLEncoder.encode(ctx.change.title, "UTF-8")

      val requestURL = s"https://en.wikipedia.org/w/api.php?format=json&action=query&prop=extracts&exlimit=max&explaintext&exintro&titles=$encodedTitle"

      val userAgent = `User-Agent`(
        "WikipediaEditsAnalyser/1.0 (https://github.com/pbernet/akka_streams_tutorial)"
      )

      Http().singleRequest(HttpRequest(uri = requestURL, headers = List(userAgent)))
        .flatMap { response =>
          val status = response.status
          val contentType = response.entity.contentType
          response.entity.toStrict(2.seconds).map(strict => (status, contentType, strict.data.utf8String))
        }
        .map { case (status, contentType, body) =>
          logger.debug(s"[${ctx.traceId}] Raw Wikipedia API response (status=$status, contentType=$contentType): $body")

          val isJson = contentType.mediaType == MediaTypes.`application/json`
          if (!status.isSuccess() || !isJson) {
            logger.error(
              s"[${ctx.traceId}] Unexpected Wikipedia API response for title: '${ctx.change.title}': " +
                s"status=$status, contentType=$contentType, bodyPreview=${body.take(120).replaceAll("\\s+", " ")}"
            )
            ctx.copy(content = "")
          } else {
            parse(body) match {
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
        }
        .recover {
          case ex: Exception =>
            logger.error(s"[${ctx.traceId}] Error fetching content from Wikipedia API: ${ex.getMessage}", ex)
            ctx.copy(content = "")
        }
    }

    private def findPersons(ctx: Ctx): Future[Ctx] =
      findPersonsLocal(ctx).flatMap(localResult => findPersonsRemoteNER(localResult))

    private def findPersonsLocal(ctx: Ctx): Future[Ctx] =
      if (ctx.content.isEmpty) Future(ctx)
      else findPersonsLocalGLiNER(ctx).recoverWith {
        case NonFatal(ex) =>
          logger.warn(s"[${ctx.traceId}] Local GLiNER NER failed (${ex.getMessage}) - falling back to Local Java NLP NER")
          findPersonsLocalNER(ctx)
      }

    private def logNERResults(ctx: Ctx): Ctx = {
      val localPersons = ctx.personsFoundLocal
      val remotePersons = ctx.personsFoundRemote
      val allPersons = (localPersons ++ remotePersons).distinct
      if (allPersons.isEmpty) return ctx

      val localOnly = localPersons.diff(remotePersons)
      val remoteOnly = remotePersons.diff(localPersons)
      val both = localPersons.intersect(remotePersons)
      def row(label: String, names: List[String]): Option[Array[String]] =
        Option.when(names.nonEmpty)(Array(label, names.size.toString, names.mkString(", ")))

      val rows = List(
        row("Total Persons", allPersons),
        row("Local NER only", localOnly),
        row("Remote NER only", remoteOnly),
        row("Found by Local & Remote", both)
      ).flatten.toArray

      val table = formatAsAsciiTable(s"NER results for '${ctx.change.title}'", Array("Category", "Count", "Names"), rows)
      logger.info(s"[${ctx.traceId}]\n$table")
      ctx
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

    private lazy val remoteNerModel = OpenAiChatModel.builder()
      .apiKey(openAiApiKey)
      .modelName(openAiModelName)
      .reasoningEffort(openAiReasoningEffort)
      .temperature(0)
      .timeout(Duration.ofSeconds(30))
      .logRequests(true)
      .build()

    private val remoteNerInstructions = SystemMessage.from(
      """Extract the names of all real persons mentioned in the text provided by the user.
        |
        |Include:
        |- Full names (e.g. "Marie Curie") and single names that clearly refer to a person (e.g. "Shakespeare")
        |
        |Exclude:
        |- Fictional characters
        |- Roles, job titles and groups of persons without a name (e.g. "the prime minister", "missionaries")
        |- Organizations, places, products and titles of works (books, films, songs)
        |
        |Output:
        |- One name per line, without numbering, bullets, quotes or explanations
        |- Omit titles and honorifics (e.g. "Dr. Johnson" -> "Johnson")
        |- List each person once, using the most complete form of the name found in the text
        |- If a name is also given in another script, return only the English form
        |- If there are no persons, return NONE""".stripMargin)

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

      Future {
        val reply = remoteNerModel.chat(remoteNerInstructions, UserMessage.from(content)).aiMessage().text()
        // Filter NONE after sanitizing, which also removes quotes around it
        sanitizePersonNames(reply.linesIterator.toList).filterNot(_.equalsIgnoreCase("NONE")).distinct
      }.map { personsFound =>
        if (personsFound.isEmpty) ctx
        else {
          logger.info(s"[${ctx.traceId}] Remote NER found persons: $personsFound from content: $content")
          ctx.copy(personsFoundRemote = personsFound)
        }
      }.recover {
        case NonFatal(e) =>
          logger.error(s"[${ctx.traceId}] Error during remote LLM call: ${e.getMessage}", e)
          ctx
      }
    }

    private def findPersonsLocalGLiNER(ctx: Ctx): Future[Ctx] = {
      logger.info(s"[${ctx.traceId}] Local GLiNER NER: About to find person names in: ${ctx.change.title}")
      val content = ctx.content
      val request = GLiNERExtractionRequest(text = content)
      val requestJson = request.asJson.noSpaces

      val httpRequest = HttpRequest(
        method = HttpMethods.POST,
        uri = glinerServiceUrl,
        entity = HttpEntity(ContentTypes.`application/json`, requestJson)
      )

      Http()
        .singleRequest(httpRequest)
        .flatMap { response =>
          response.status match {
            case StatusCodes.OK =>
              Unmarshal(response.entity).to[String].map { jsonString =>
                logger.debug(s"[${ctx.traceId}] GLiNER API response: $jsonString")

                decode[GLiNERExtractionResponse](jsonString) match {
                  case Right(glinerResponse) =>
                    val personsFound = glinerResponse.persons.map(_.text).distinct

                    if (personsFound.isEmpty) {
                      logger.debug(s"[${ctx.traceId}] GLiNER found no persons in content")
                      ctx
                    } else {
                      val personsFoundCleaned = sanitizePersonNames(personsFound)
                      logger.info(s"[${ctx.traceId}] Local GLiNER NER found persons: $personsFoundCleaned from content: ${content.take(100)}...")
                      ctx.copy(personsFoundLocal = personsFoundCleaned)
                    }
                  case Left(error) =>
                    throw new IllegalStateException(s"Invalid GLiNER response: $error")
                }
              }
            case statusCode =>
              response.discardEntityBytes()
              Future.failed(new IllegalStateException(s"GLiNER service returned error status: $statusCode"))
          }
        }
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

    // Title prefixes (namespaces) we don't want to fetch extracts for
    private val excludedTitlePrefixes: Seq[String] = Seq(
      "Category:", "Kategorie:", "Catégorie:", "Categoría:", "Categoria:",
      "تصنيف:", "Категория:", "分类:", "分類:",
      "File:", "Datei:", "Fichier:", "Archivo:", "ファイル:",
      "Talk:", "User:", "User talk:", "Wikipedia:", "Help:",
      "Template:", "Portal:", "Module:", "Draft:", "MediaWiki:", "Special:",
      "Комментарии:"
    )

    // Wikidata-style identifiers like Q1450397, P31
    private val wikidataIdRegex = "^[A-Z]\\d+$".r

    private def isInterestingTitle(title: String): Boolean = {
      if (title == null || title.isBlank) false
      else if (excludedTitlePrefixes.exists(p => title.regionMatches(true, 0, p, 0, p.length))) false
      else if (wikidataIdRegex.matches(title)) false
      // Require at least one ASCII letter to keep the EN-focused pipeline meaningful
      else if (!title.matches(".*[A-Za-z].*")) false
      else true
    }

    private val titleFilterFlow: Flow[Change, Change, NotUsed] = Flow[Change]
      .filter { change =>
        val keep = isInterestingTitle(change.title)
        if (!keep) logger.debug(s"[${change.traceId}] Dropping uninteresting title: ${change.title}")
        keep
      }

    private val englishWikipediaFilterFlow: Flow[Change, Change, NotUsed] =
      Flow[Change].filter(_.wiki == "enwiki")

    private val nerProcessingFlow: Flow[Change, Ctx, NotUsed] = Flow[Change]
      .filter(change => !change.isBot)
      .map(change => Ctx(change))
      // Decouple from the SSE source so bursts don't backpressure (and time out) the upstream
      // On overflow: drop the OLDEST queued items - we prefer freshness for a live edits feed
      .buffer(100, OverflowStrategy.dropHead)
      // Respect Wikimedia API rate limits (max: 200 req / Min for clients with user agent set)
      // https://www.mediawiki.org/wiki/Wikimedia_APIs/Rate_limits
      .throttle(180, 1.minute, 1, ThrottleMode.Shaping)
      .mapAsync(2)(ctx => fetchContent(ctx))
      .mapAsync(2)(ctx => findPersons(ctx))
      .map(ctx => logNERResults(ctx))

    private val embeddingStoreSink = Flow[Ctx]
      .filter(ctx => ctx.content.nonEmpty)
      .map(ctx => addToEmbeddingStore(ctx.change.title, ctx.content))
      .to(Sink.ignore)


    logger.info(s"Opensearch container listening on: ${searchContainer.getHttpHostAddress}")
    logger.info(s"Local NER: GLiNER service expected at: $glinerServiceUrl (fallback: Local Java NLP NER)")
    logger.info("About to start processing flow...")

    restartSource
      .via(parserFlow)
      .via(englishWikipediaFilterFlow)
      .via(titleFilterFlow)
      .via(nerProcessingFlow)
      .alsoTo(embeddingStoreSink)
      // One document per article: a new edit of the same article replaces the previous document
      .map(ctx => createIndexMessage(ctx.change.title, ctx))
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
      val model = OpenAiChatModel.builder.apiKey(openAiApiKey).modelName(openAiModelName).reasoningEffort(openAiReasoningEffort).logRequests(true).build

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


    // A search hit is an article, which may mention other persons too
    // Rank names matching the query first (lower is better), so the searched person is on top
    private def matchRank(name: String, query: String): Int = {
      val nameLower = name.toLowerCase
      val queryLower = query.trim.toLowerCase
      val nameWords = nameLower.split("\\s+")
      val queryWords = queryLower.split("\\s+").filter(_.nonEmpty)

      def startsAWord(queryWord: String) = nameWords.exists(_.startsWith(queryWord))

      if (nameLower == queryLower) 0
      else if (nameLower.startsWith(queryLower)) 1
      else if (queryWords.forall(nameWords.contains)) 2
      else if (queryWords.forall(startsAWord)) 3
      else if (queryWords.exists(startsAWord)) 4
      else 5
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
        // Build via circe, so that user input such as " or \ is JSON escaped
        Json.obj(
          "simple_query_string" -> Json.obj(
            "fields" -> List("personsFoundLocal", "personsFoundRemote").asJson,
            "query" -> s"$query*".asJson
          )
        ).noSpaces
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
            readResult.source.personsFoundLocal ++ readResult.source.personsFoundRemote
          }.distinct
          allPersonsFound.sortBy(name => (matchRank(name, query), name)).map(Person(_)).toList
        }
        .recover {
          case ex =>
            logger.error(s"Error searching persons with query: $query", ex)
            List.empty[Person]
        }
    }

    private final case class QueryRequest(query: String, selectedPerson: Option[Person] = None) {
      // Name the selected person in the text, so that both retrieval and the LLM know who a follow-up is about
      def queryWithPerson: String = selectedPerson match {
        case Some(person) if !query.toLowerCase.contains(person.name.toLowerCase) => s"About ${person.name}: $query"
        case _ => query
      }
    }

    private final case class QueryResponse(answer: String)

    private final case class PersonSearchRequest(query: String)

    private final case class PersonSearchResponse(persons: List[Person])

    private final case class ProcessingControlRequest(enabled: Boolean)

    private final case class ProcessingControlResponse(enabled: Boolean, message: String)

    private final case class SearchIndexUrlResponse(url: String)

    private def startConversationWith(assistant: Assistant): Unit = {
      def remoteNerStatus(enabled: Boolean): String =
        if (enabled) "Remote NER enabled" else "Remote NER paused"

      def setProcessing(enable: Boolean): ProcessingControlResponse = {
        val changed = isRemoteProcessingEnabled.getAndSet(enable) != enable
        val status = remoteNerStatus(enable)
        if (changed) logger.info(status)
        ProcessingControlResponse(enable, status)
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
                        searchPersons(request.query).map(persons =>
                          PersonSearchResponse(persons)
                        ).map(response =>
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
                      val answer = assistant.answer(queryRequest.queryWithPerson)
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
                        val response = setProcessing(controlRequest.enabled)
                        complete(HttpEntity(ContentTypes.`application/json`, response.asJson.noSpaces))
                      case Left(error) =>
                        complete(HttpResponse(400, entity = s"Invalid JSON: $error"))
                    }
                  }
                },
                get {
                  val enabled = isRemoteProcessingEnabled.get()
                  val response = ProcessingControlResponse(enabled, remoteNerStatus(enabled))
                  complete(HttpEntity(ContentTypes.`application/json`, response.asJson.noSpaces))
                }
              )
            },
            path("searchIndexUrl") {
              get {
                val url = s"http://localhost:${searchContainer.getMappedPort(9200)}/$indexName/_search?q=personsFoundLocal:*%20OR%20personsFoundRemote:*&size=100&pretty=true"
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

    // Note that the size of the collection can also be fetched via a GET request, e.g.
    // http://localhost:57321/wikipediaedits/_count
    private def query(): Unit = {
      logger.info(s"About to execute scrolled read queries...")
      for {
        result <- elasticsearchSourceTyped.runWith(Sink.seq)
        resultRaw <- elasticsearchSourceRaw.runWith(Sink.seq)
      } {
        logger.info(s"Read typed: ${result.size}. 1st element: ${result.head}")
        logger.info(s"Read raw: ${resultRaw.size}. 1st element: ${resultRaw.head}")
      }
    }

    private def addToEmbeddingStore(title: String, content: String): Unit = {
      val document = Document.from(content)
      val splitter = DocumentSplitters.recursive(300, 0)
      val segments = splitter
        .split(document)
        .stream()
        .map(segment =>
          TextSegment.from(
            s"Title: $title\n\n${segment.text()}",
            segment.metadata()
          )
        )
        .toList
      val embeddings = embeddingModel.embedAll(segments).content()
      embeddingStore.addAll(embeddings, segments)
    }
  }
}

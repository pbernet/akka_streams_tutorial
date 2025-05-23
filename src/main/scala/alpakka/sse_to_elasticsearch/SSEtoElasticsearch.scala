package alpakka.sse_to_elasticsearch

import dev.langchain4j.data.document.Document
import dev.langchain4j.data.document.splitter.DocumentSplitters
import dev.langchain4j.data.message.UserMessage
import dev.langchain4j.data.segment.TextSegment
import dev.langchain4j.memory.chat.MessageWindowChatMemory
import dev.langchain4j.model.embedding.onnx.bgesmallenv15q.BgeSmallEnV15QuantizedEmbeddingModel
import dev.langchain4j.model.openai.OpenAiChatModel
import dev.langchain4j.model.openai.OpenAiChatModelName.GPT_4_O_MINI
import dev.langchain4j.rag.DefaultRetrievalAugmentor
import dev.langchain4j.rag.content.retriever.EmbeddingStoreContentRetriever
import dev.langchain4j.rag.query.router.DefaultQueryRouter
import dev.langchain4j.service.AiServices
import dev.langchain4j.store.embedding.inmemory.InMemoryEmbeddingStore
import io.circe.*
import io.circe.parser.*
import opennlp.tools.namefind.{NameFinderME, TokenNameFinderModel}
import opennlp.tools.tokenize.{TokenizerME, TokenizerModel}
import opennlp.tools.util.Span
import org.apache.commons.text.StringEscapeUtils
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.sse.ServerSentEvent
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, HttpRequest}
import org.apache.pekko.http.scaladsl.server.Directives.{as, complete, concat, entity, get, getFromFile, path, pathEndOrSingleSlash, pathPrefix, post}
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
import java.time.{Instant, ZoneId}
import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.sys.process.{Process, stringSeqToProcess}
import scala.util.control.NonFatal

/**
  * Consume Wikipedia edits via SSE (like in [[alpakka.sse.SSEClientWikipediaEdits]]),
  * fetch the abstract via Wikipedia API,
  * do local and remote NER processing for persons in EN
  * and then write the results to either:
  *  - Elasticsearch version 7.x server
  *  - Opensearch version 2.x server
  *    Also write the edits as embeddings to a local [[InMemoryEmbeddingStore]]
  *    to be able to RAG chat with them via a local [[Assistant]]
  *
  * Remarks:
  *  - We use [[spray.json]] because of the elasticsearch pekko connectors
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

object SSEtoElasticsearch extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  private val decider: Supervision.Decider = {
    case NonFatal(e) =>
      logger.warn(s"Stream failed with: $e, going to restart")
      Supervision.Restart
  }

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

  case class Change(timestamp: Long, title: String, serverName: String, user: String, cmdType: String, isBot: Boolean, isNamedBot: Boolean, lengthNew: Int = 0, lengthOld: Int = 0)

  object Change extends ((Long, String, String, String, String, Boolean, Boolean, Int, Int) => Change) {
    def apply(timestamp: Long, title: String, serverName: String, user: String, cmdType: String, isBot: Boolean, isNamedBot: Boolean, lengthNew: Int = 0, lengthOld: Int = 0): Change =
      new Change(timestamp, title, serverName, user, cmdType, isBot, isNamedBot, lengthNew, lengthOld)

    implicit def formatChange: RootJsonFormat[Change] = jsonFormat9(Change.apply)
  }

  // Helps to carry the data through the stages, although this violates functional principles
  case class Ctx(change: Change, personsFound: List[String] = List.empty, personsFoundRemote: List[String] = List.empty, content: String = "")

  private object Ctx extends ((Change, List[String], List[String], String) => Ctx) {
    def apply(change: Change, personsFound: List[String] = List.empty, personsFoundRemote: List[String] = List.empty, content: String = ""): Ctx =
      new Ctx(change, personsFound, personsFoundRemote, content)

    implicit def formatCtx: RootJsonFormat[Ctx] = jsonFormat4(Ctx.apply)
  }

  //  private val dockerImageName = DockerImageName
  //    .parse("docker.elastic.co/elasticsearch/elasticsearch-oss")
  //    .withTag("7.10.2")
  //  private val elasticsearchContainer = new ElasticsearchContainer(dockerImageName)
  //  elasticsearchContainer.start()
  private val dockerImageNameOS = DockerImageName
    .parse("opensearchproject/opensearch")
    .withTag("2.18.0")
  private val searchContainer = new OpensearchContainer(dockerImageNameOS)
  searchContainer.start()

  val address = searchContainer.getHttpHostAddress
  //val connectionSettings = ElasticsearchConnectionSettings(s"http://$address")
  val connectionSettings = OpensearchConnectionSettings(s"$address")
    .withCredentials("user", "password")

  // This index will be created in Elasticsearch on the fly
  private val indexName = "wikipediaedits"
  //private val searchParams = ElasticsearchParams.V7(indexName)
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

  private def fetchContent(ctx: Ctx): Future[Ctx] = {
    logger.info(s"About to read `extract` from Wikipedia entry with title: ${ctx.change.title}")
    val encodedTitle = URLEncoder.encode(ctx.change.title, "UTF-8")

    val requestURL = s"https://en.wikipedia.org/w/api.php?format=json&action=query&prop=extracts&exlimit=max&explaintext&exintro&titles=$encodedTitle"
    Http().singleRequest(HttpRequest(uri = requestURL))
      // Consume the streamed response entity
      // Doc: https://doc.akka.io/docs/akka-http/current/client-side/request-level.html
      .flatMap(_.entity.toStrict(2.seconds))
      .map(_.data.utf8String.split("\"extract\":").reverse.head)
      .map(content => ctx.copy(content = content))
  }

  private def findPersons(ctx: Ctx): Future[Ctx] = {
    findPersonsLocalNER(ctx).flatMap { localResult =>
      findPersonsRemoteNER(localResult).map { remoteResult =>
        val localPersons = localResult.personsFound
        val remotePersons = remoteResult.personsFoundRemote
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
            rowsListToDisplay += Array("Local NER only", localOnly.size.toString, localOnly.mkString(", "))
          }
          if (remoteOnly.nonEmpty) {
            rowsListToDisplay += Array("Remote NER only", remoteOnly.size.toString, remoteOnly.mkString(", "))
          }
          if (both.nonEmpty) {
            rowsListToDisplay += Array("Found by both", both.size.toString, both.mkString(", "))
          }
          val table = formatAsAsciiTable(title, headers, rowsListToDisplay.toArray)
          logger.info(s"\n$table")

          ctx.copy(personsFound = allPersons, personsFoundRemote = remotePersons)
        } else {
          ctx
        }
      }
    }
  }

  private def findPersonsLocalNER(ctx: Ctx): Future[Ctx] = {
    logger.info(s"Local NER: About to find person names in: ${ctx.change.title}")
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
      val personsFoundCleaned = personsFound.map(each => StringEscapeUtils.unescapeJava(each))
      logger.debug(s"Local NER found persons: $personsFoundCleaned from content: $content")
      Future(ctx.copy(personsFound = personsFoundCleaned))
    }
  }

  private def findPersonsRemoteNER(ctx: Ctx): Future[Ctx] = {
    logger.info(s"Remote NER: About to find person names in: ${ctx.change.title}")
    val content = ctx.content

    if (content.isEmpty) {
      return Future(ctx)
    }

    val model = OpenAiChatModel.builder()
      .apiKey(OPENAI_API_KEY)
      .modelName(GPT_4_O_MINI)
      .build()

    val promptPersons =
      """Extract all person names from the following text.
        |Return only the names, one per line. If no names are found, return an empty string.
        |Do NOT include organizations, places, products, or other entities
        |
        |Text: {{content}}
        |
        |Person names:""".stripMargin

    val message = UserMessage.from(promptPersons.replace("{{content}}", content))

    try {
      val response = model.chat(message)
      val personsFoundText = response.aiMessage().text().trim()

      val personsFoundList = if (personsFoundText.isEmpty) {
        List.empty[String]
      } else {
        personsFoundText.split("\n").map(_.trim).filter(_.nonEmpty).toList
      }

      if (personsFoundList.isEmpty) {
        Future(ctx)
      } else {

        logger.debug(s"Remote NER found persons: $personsFoundList from content: $content")
        Future(ctx.copy(personsFoundRemote = personsFoundList))
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error during remote LLM call: ${e.getMessage}", e)
        Future(ctx)
    }
  }

  /**
    * Formats data as an ASCII table with proper borders and alignment.
    *
    * @param title   The title to display at the top of the table
    * @param headers Column headers
    * @param data    Table data as rows of columns
    * @return A formatted ASCII table as a string
    */
  private def formatAsAsciiTable(title: String, headers: Array[String], data: Array[Array[String]]): String = {
    // Calculate column widths (max width of each column)
    val colWidths = headers.indices.map { i =>
      val headerWidth = headers(i).length
      val maxDataWidth = data.map(_(i).length).max
      math.max(headerWidth, maxDataWidth) + 2 // +2 for padding
    }.toArray

    val totalWidth = colWidths.sum + colWidths.length + 1
    val border = "+" + colWidths.map(w => "-" * w).mkString("+") + "+"

    def formatRow(row: Array[String]): String = {
      "| " + row.zip(colWidths).map { case (cell, width) =>
        cell.padTo(width - 1, ' ') + " "
      }.mkString("| ") + "|"
    }

    val tableBuilder = new StringBuilder
    tableBuilder.append(border).append("\n")
    tableBuilder.append("| ").append(title.padTo(totalWidth - 4, ' ')).append(" |").append("\n")
    tableBuilder.append(border).append("\n")
    tableBuilder.append(formatRow(headers)).append("\n")
    tableBuilder.append(border).append("\n")
    data.foreach(row => tableBuilder.append(formatRow(row)).append("\n"))
    tableBuilder.append(border)

    tableBuilder.toString
  }

  private val nerProcessingFlow: Flow[Change, Ctx, NotUsed] = Flow[Change]
    .filter(change => !change.isBot)
    .map(change => Ctx(change))
    .mapAsync(3)(ctx => fetchContent(ctx))
    .mapAsync(3)(ctx => findPersons(ctx))
    .filter(ctx => ctx.personsFound.nonEmpty)

  private val embeddingStoreSink = Flow[Ctx]
    .filter(ctx => ctx.content.nonEmpty)
    .map(ctx => addToEmbeddingStore(ctx.content))
    .to(Sink.ignore)


  logger.info(s"Elasticsearch/Opensearch container listening on: ${searchContainer.getHttpHostAddress}")
  logger.info("About to start processing flow...")

  restartSource
    .via(parserFlow)
    .via(nerProcessingFlow)
    .alsoTo(embeddingStoreSink)
    .map(ctx => createIndexMessage(dateTimeFormatted(ctx.change.timestamp), ctx))
    .wireTap(each => logger.debug(s"Add to index: $each"))
    .withAttributes(ActorAttributes.supervisionStrategy(decider))
    .runWith(elasticsearchSink)



  // Wait for the index to populate
  Thread.sleep(10.seconds.toMillis)
  indexClient()
  aiClient()

  Source.tick(1.seconds, 10.seconds, ())
    .map(_ => query())
    .runWith(Sink.ignore)

  private def indexClient() = {
    val os = System.getProperty("os.name").toLowerCase
    val url = s"http://localhost:${searchContainer.getMappedPort(9200)}/$indexName/_search?q=personsFound:*&size=100"
    if (os == "mac os x") Process(s"open $url").!
    else if (os.startsWith("windows")) Seq("cmd", "/c", s"start $url").!
    else logger.info(s"Please open a browser at: $url")
  }

  private def aiClient() = {
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

  private def startConversationWith(assistant: Assistant): Unit = {
    final case class QueryRequest(query: String)
    final case class QueryResponse(answer: String)

    import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport.*
    import spray.json.*
    import DefaultJsonProtocol.*

    implicit val queryRequestFormat: RootJsonFormat[QueryRequest] = jsonFormat1(QueryRequest.apply)
    implicit val queryResponseFormat: RootJsonFormat[QueryResponse] = jsonFormat1(QueryResponse.apply)

    val route: Route =
      pathPrefix("assistant") {
        concat(
          path("query") {
            post {
              entity(as[QueryRequest]) { queryRequest =>
                val answer = assistant.answer(queryRequest.query)
                val response = QueryResponse(answer)
                complete(HttpEntity(ContentTypes.`application/json`, response.toJson.toString))
              }
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

  // Note that the size of the collection can also be fetched via a GET request, eg
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
package rag.core

import dev.langchain4j.data.document.Document
import dev.langchain4j.data.document.splitter.DocumentSplitters
import dev.langchain4j.memory.ChatMemory
import dev.langchain4j.memory.chat.{ChatMemoryProvider, MessageWindowChatMemory}
import dev.langchain4j.model.cohere.CohereScoringModel
import dev.langchain4j.model.embedding.onnx.bgesmallenv15q.BgeSmallEnV15QuantizedEmbeddingModel
import dev.langchain4j.model.openai.OpenAiChatModel
import dev.langchain4j.model.openai.OpenAiChatModelName.GPT_4_O_MINI
import dev.langchain4j.rag.DefaultRetrievalAugmentor
import dev.langchain4j.rag.content.aggregator.{DefaultContentAggregator, ReRankingContentAggregator}
import dev.langchain4j.rag.query.router.DefaultQueryRouter
import dev.langchain4j.service.{AiServices, MemoryId, UserMessage}
import dev.langchain4j.store.embedding.EmbeddingSearchRequest
import dev.langchain4j.store.embedding.pgvector.PgVectorEmbeddingStore
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

case class DocumentInfo(name: String, chunks: Int)

case class RagStatus(
                      status: String,
                      message: String,
                      documentsIndexed: Int,
                      totalChunks: Int,
                      ingesting: Boolean,
                      documentsProcessed: Int,
                      documentsTotal: Int,
                      rerankerEnabled: Boolean,
                      rerankerMinScore: Double,
                      rerankerModelName: String
                    )
object RagStatus {
  implicit val encoder: Encoder[RagStatus] = deriveEncoder[RagStatus]
}

trait ChatAssistant {
  def chat(@MemoryId sessionId: Object, @UserMessage(Array("{{query}}")) query: String): String
}

/**
  * Configuration for Cohere reranking behavior
  *
  * @param enabled   Whether Cohere reranking is enabled
  * @param minScore  Minimum relevance score (0-1) for reranked results
  * @param modelName Cohere rerank model to use (default: rerank-english-v3.0)
  */
case class RerankerConfig(
                           enabled: Boolean = false,
                           minScore: Double = 0.01,
                           modelName: String = "rerank-english-v3.0"
                         )


/**
  * Core RAG Engine for document indexing and question answering.
  *
  * RagEngine manages document indexing, embedding storage,
  * semantic search with optional reranking, and LLM-powered question answering.
  * It uses PostgreSQL with pgvector for embedding storage
  * Supported document formats: PDF, DOCX, DOC, TXT
  *
  * Internal Retrieval Pipeline:
  * 1. Query embedding generated using BGE-small-en-v1.5 (384-dim)
  * 2. Vector similarity search in pgvector
  * 3. Optional Cohere reranking (if enabled) re-scores results by semantic relevance
  * 4. Top-ranked chunks passed to OpenAI LLM with system prompt for RAG-grounded responses
  * 5. Source attribution captured from pre-reranked retrieval results
  *
  * Configuration:
  * Environment Variables:
  * - DOCUMENTS_PATH: Override default documents directory (default: src/main/resources/content)
  * - OPENAI_API_KEY: [required] OpenAI API key for chat model
  * - COHERE_API_KEY: [required if reranking enabled] Cohere API key for semantic reranking
  *
  * Prerequisites:
  * - PostgreSQL with pgvector extension running (see docker-compose-rag.yml)
  * - Content files are in DOCUMENTS_PATH
  * - OPENAI_API_KEY environment variable must be set
  */
class RagEngine(
                 documentsPath: String = sys.env.getOrElse("DOCUMENTS_PATH", "src/main/resources/content"),
                 dbHost: String = "localhost",
                 dbPort: Int = 5432,
                 dbName: String = "ragdb",
                 dbUser: String = "postgres",
                 dbPassword: String = "postgres",
                 openAiApiKey: String = sys.env.getOrElse("OPENAI_API_KEY", ""),
                 dropTableFirst: Boolean = true,
                 initialRerankerConfig: RerankerConfig = RerankerConfig()
               )(implicit system: ActorSystem) {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  if (openAiApiKey.isEmpty) {
    throw new IllegalStateException("OPENAI_API_KEY environment variable not set!")
  }

  @volatile private var rerankerConfig: RerankerConfig = initialRerankerConfig

  private val embeddingStore = PgVectorEmbeddingStore.builder()
    .host(dbHost)
    .port(dbPort)
    .database(dbName)
    .user(dbUser)
    .password(dbPassword)
    .table("embeddings")
    .dimension(384)
    .createTable(true)
    .dropTableFirst(dropTableFirst)
    .build()

  private val embeddingModel = new BgeSmallEnV15QuantizedEmbeddingModel()

  private val contentRetriever = new ScoringContentRetriever(
    embeddingStore = embeddingStore,
    embeddingModel = embeddingModel,
    maxResults = 10,
    minScore = 0.6
  )

  @volatile private var indexedDocuments: List[DocumentInfo] = List.empty
  @volatile private var ingesting: Boolean = false
  @volatile private var documentsProcessed: Int = 0
  @volatile private var documentsTotal: Int = 0
  @volatile private var assistant: ChatAssistant = _
  @volatile private var loggingAggregator: LoggingContentAggregator = _
  // TODO sessionMemories grows unboundedly, no explicit removal yet
  // Replace with cache with TTL-based eviction (e.g. Caffeine cache with expireAfterAccess).
  private val sessionMemories = new ConcurrentHashMap[Object, ChatMemory]()

  private val supportedExtensions = Set(".pdf", ".docx", ".doc", ".txt")

  /**
    * Initialize the RAG Engine by loading all documents from the configured directory
    * and setting up the LLM assistant with retrieval augmentation.
    * Must be called before any queries can be processed.
    *
    * Connects to PostgreSQL, loads and chunks document files, generates embeddings,
    * and initializes the OpenAI chat model with RAG context.
    *
    * @throws IllegalStateException if OPENAI_API_KEY was not provided at construction
    */
  def initialize(): Unit = {
    logger.info("Initializing RAG Engine...")
    logger.info(s"Connecting to PostgreSQL at $dbHost:$dbPort/$dbName")
    logger.info(s"Documents path: $documentsPath")

    if (rerankerConfig.enabled) {
      logger.info(s"Cohere reranking enabled with model: ${rerankerConfig.modelName}, minScore: ${rerankerConfig.minScore}")
    } else {
      logger.info("Cohere reranking disabled")
    }

    assistant = createAssistant()
    logger.info("RAG Engine initialized - assistant ready, starting async document ingestion...")

    startAsyncIngestion()
  }

  /**
    * Query the RAG system and get the answer with source attribution.
    *
    * Executes the full retrieval augmentation pipeline: embedding query, vector search,
    * optional reranking, and LLM inference. Returns both the generated answer and the
    * source chunks used for context.
    *
    * Source attribution is always captured from the [[LoggingContentAggregator]],
    * which wraps either a [[ReRankingContentAggregator]] (when reranking is enabled)
    * or a [[DefaultContentAggregator]] (passthrough). Each source chunk is marked
    * with reranked=true if it survived Cohere reranking.
    *
    * @param query The question to ask
    * @return RichChatResponse containing answer and source chunks with metadata
    * @throws IllegalStateException if initialize() has not been called
    */
  private val aggregatorLock = new java.util.concurrent.locks.ReentrantLock()

  def chatWithMetadata(query: String, sessionId: String): RichChatResponse = {
    if (assistant == null) {
      throw new IllegalStateException("RagEngine not initialized. Call initialize() first.")
    }

    logger.info(s"Processing query with metadata capture: $query")

    aggregatorLock.lock()
    try {
      loggingAggregator.clearStats()

      val answer = assistant.chat(sessionId, query)

      val sourceChunks = loggingAggregator.getSourceChunks
      val rerankingStats = loggingAggregator.getLastStats
      logger.info(s"Retrieved: ${sourceChunks.length} chunks from aggregator (${sourceChunks.count(_.reranked)} marked as reranked)")

      val response = RichChatResponse(
        answer = answer,
        sources = sourceChunks,
        totalChunksRetrieved = sourceChunks.length,
        rerankingStats = rerankingStats
      )

      logger.info(s"Built RichChatResponse: ${response.rerankingSummary}")
      response

    } catch {
      case ex: Exception =>
        logger.error(s"Error during chatWithMetadata: ${ex.getMessage}", ex)
        throw ex
    } finally {
      aggregatorLock.unlock()
    }
  }

  /**
    * Prepare a query for external LLM processing without calling the LLM.
    *
    * Executes the retrieval pipeline (embedding query, vector search) and assembles
    * a complete prompt with system message, retrieved context, and user query.
    * The MCP client can then send this prepared query to their own LLM.
    *
    * @param query The question to prepare
    * @return RichChatResponse with preparedQuery containing the assembled prompt,
    *         sources with full chunk details, but empty answer field
    * @throws IllegalStateException if initialize() has not been called
    */
  def prepareQuery(query: String): RichChatResponse = {
    if (assistant == null) {
      throw new IllegalStateException("RagEngine not initialized. Call initialize() first.")
    }

    logger.info(s"Preparing query without LLM call: $query")

    try {
      val queryEmbedding = embeddingModel.embed(query).content()
      val searchRequest = EmbeddingSearchRequest.builder()
        .queryEmbedding(queryEmbedding)
        .maxResults(10)
        .build()

      val matches = embeddingStore.search(searchRequest).matches().asScala.toList
      logger.info(s"Retrieved: ${matches.length} matching segments for prepareQuery")

      val sourceChunks = matches.zipWithIndex.map { case (match_, idx) =>
        val segment = match_.embedded()
        val metadata = segment.metadata()
        val pageNumbers = Option(metadata.getString("pageNumbers"))
          .filter(_.nonEmpty)
          .map(_.split(",").flatMap(s => Try(s.trim.toInt).toOption).toList)
          .getOrElse(Nil)

        SourceChunk(
          fileName = Option(metadata.getString("fileName")).getOrElse("unknown.pdf"),
          pageCount = Try(Option(metadata.getString("pageCount")).getOrElse("0").toInt).getOrElse(0),
          chunkIndex = idx,
          chunkText = segment.text(),
          score = match_.score(),
          title = Option(metadata.getString("title")),
          author = Option(metadata.getString("author")),
          creationDate = Option(metadata.getString("creationDate")),
          subject = Option(metadata.getString("subject")),
          pageNumbers = pageNumbers
        )
      }

      val contextText = sourceChunks.zipWithIndex.map { case (chunk, idx) =>
        val sourceInfo = if (chunk.fileName.nonEmpty) s" (from: ${chunk.fileName})" else ""
        s"[${idx + 1}]$sourceInfo:\n${chunk.chunkText}"
      }.mkString("\n\n")

      val preparedPrompt =
        s"""${RagEngine.SystemPrompt}
           |
           |Context from documents:
           |$contextText
           |
           |User question: $query""".stripMargin

      logger.info(s"Prepared query with: ${sourceChunks.length} context chunks")

      RichChatResponse(
        answer = "",
        sources = sourceChunks,
        totalChunksRetrieved = sourceChunks.length,
        rerankingStats = None,
        preparedQuery = Some(preparedPrompt)
      )
    } catch {
      case ex: Exception =>
        logger.error(s"Error during prepareQuery: ${ex.getMessage}", ex)
        throw ex
    }
  }

  /**
    * Get current RAG system status as structured data.
    *
    * @return RagStatus containing operational state, document count, and total chunks
    */
  def getStatus: RagStatus = {
    val statusStr = if (ingesting) "ingesting"
    else if (indexedDocuments.nonEmpty) "ready"
    else "no_documents"
    val msg = if (ingesting)
      s"Ingesting documents: $documentsProcessed/$documentsTotal"
    else if (indexedDocuments.nonEmpty)
      s"${indexedDocuments.size} documents indexed"
    else
      "No documents indexed"
    RagStatus(
      status = statusStr,
      message = msg,
      documentsIndexed = indexedDocuments.size,
      totalChunks = indexedDocuments.map(_.chunks).sum,
      ingesting = ingesting,
      documentsProcessed = documentsProcessed,
      documentsTotal = documentsTotal,
      rerankerEnabled = rerankerConfig.enabled,
      rerankerMinScore = rerankerConfig.minScore,
      rerankerModelName = rerankerConfig.modelName
    )
  }

  /**
    * Get the list of indexed documents as structured data.
    *
    * @return List of DocumentInfo containing name and chunk count per document
    */
  def getIndexedDocuments: List[DocumentInfo] = indexedDocuments

  /**
    * Get current reranker configuration.
    *
    * @return RerankerConfig with enabled state and minScore threshold
    */
  def getRerankerConfig: RerankerConfig = rerankerConfig

  /**
    * Set the minimum relevance score threshold for Cohere reranking.
    *
    * Takes effect immediately by recreating the assistant.
    *
    * @param newMinScore Threshold value between 0.0 (include all) and 1.0 (only exact matches)
    * @throws IllegalArgumentException if value is outside valid range [0.0, 1.0]
    */
  def setMinScore(newMinScore: Double): Unit = {
    if (newMinScore < 0.0 || newMinScore > 1.0) {
      throw new IllegalArgumentException(s"minScore must be between 0.0 and 1.0, got: $newMinScore")
    }
    rerankerConfig = rerankerConfig.copy(minScore = newMinScore)
    logger.info(s"Updated minScore to: ${f"$newMinScore%.2f"}")
    assistant = createAssistant()
  }

  /**
    * Enable or disable Cohere semantic reranking.
    *
    * When disabled, results are ranked purely by vector similarity. When enabled,
    * Cohere model re-scores retrieved chunks for improved semantic relevance.
    * Takes effect immediately by recreating the assistant.
    *
    * @param enabled Whether to enable reranking
    */
  def setRerankerEnabled(enabled: Boolean): Unit = {
    rerankerConfig = rerankerConfig.copy(enabled = enabled)
    logger.info(s"Reranking enabled: $enabled")
    assistant = createAssistant()
  }

  /**
    * Clear chat memory by recreating the assistant with fresh conversation state.
    *
    * Takes effect immediately on the next query. Useful for starting new conversations
    * without reloading documents or reconnecting to the database.
    */
  def clearChatMemory(sessionId: String): Unit = {
    logger.info(s"Clearing chat memory for session: $sessionId")
    Option(sessionMemories.remove(sessionId)).foreach(_.clear())
    logger.info(s"Chat memory cleared for session: $sessionId")
  }


  private def processDocument(path: Path): DocumentInfo = {
    val fileName = path.getFileName.toString
    if (DoclingChunkingService.isAvailable) {
      processWithDocling(path, fileName)
    } else {
      logger.warn(s"Docling server unavailable, falling back to Tika for: $fileName")
      processWithParser(path, fileName)
    }
  }

  private def processWithDocling(path: Path, fileName: String): DocumentInfo = {
    DoclingChunkingService.chunkToTextSegments(path) match {
      case Success(segments) if segments.nonEmpty =>
        logger.info(s"Docling chunked: $fileName into: ${segments.size} chunks (skipping local DocumentSplitter)")
        segments.zipWithIndex.foreach { case (segment, idx) =>
          val preview = segment.text().take(200).replaceAll("\\s+", " ")
          logger.debug(s"Docling chunk [$idx]: $preview${if (segment.text().length > 200) "..." else ""}")
        }

        val embeddings = embeddingModel.embedAll(segments.asJava).content()
        embeddingStore.addAll(embeddings, segments.asJava)

        logger.info(s"Indexed file: $fileName with: ${segments.size} chunks")
        DocumentInfo(fileName, segments.size)

      case Success(_) =>
        logger.warn(s"Docling returned no chunks for: $fileName, falling back to parser")
        processWithParser(path, fileName)

      case Failure(ex) =>
        logger.warn(s"Docling chunking failed for $fileName: ${ex.getMessage}, falling back to parser")
        processWithParser(path, fileName)
    }
  }

  private def processWithParser(path: Path, fileName: String): DocumentInfo = {
    ParserFactory.getParserFor(path) match {
      case Some(parser) =>
        parser.parse(path) match {
          case Success((text, metadata)) =>
            if (text.nonEmpty) {
              val document = Document.from(text)
              document.metadata().put("fileName", metadata.fileName)
              metadata.pageCount.foreach(p => document.metadata().put("pageCount", p.toString))
              metadata.title.foreach(t => document.metadata().put("title", t))
              metadata.author.foreach(a => document.metadata().put("author", a))
              metadata.subject.foreach(s => document.metadata().put("subject", s))
              metadata.creationDate.foreach(d => document.metadata().put("creationDate", d))
              metadata.producerApp.foreach(p => document.metadata().put("producer", p))

              val splitter = DocumentSplitters.recursive(300, 50)
              val segments = splitter.split(document).asScala.toList

              logger.info(s"Split file: $fileName into: ${segments.size} chunks with embedded metadata")

              val embeddings = embeddingModel.embedAll(segments.asJava).content()
              embeddingStore.addAll(embeddings, segments.asJava)

              logger.info(s"Indexed file: $fileName with: ${segments.size} chunks")
              DocumentInfo(fileName, segments.size)
            } else {
              logger.warn(s"No text extracted from file: $fileName")
              DocumentInfo(fileName, 0)
            }
          case Failure(_) =>
            DocumentInfo(fileName, 0)
        }
      case None =>
        logger.warn(s"No parser available for file: $fileName")
        DocumentInfo(fileName, 0)
    }
  }

  private def startAsyncIngestion(): Unit = {
    val documentsDir = Paths.get(documentsPath)

    if (!Files.exists(documentsDir)) {
      logger.warn(s"Documents directory does not exist: $documentsPath")
      Files.createDirectories(documentsDir)
      logger.info(s"Created documents directory: $documentsPath")
      return
    }

    val documentFiles = Files.list(documentsDir)
      .iterator()
      .asScala
      .filter { path =>
        val fileName = path.toString.toLowerCase
        supportedExtensions.exists(fileName.endsWith)
      }
      .toList

    if (documentFiles.isEmpty) {
      logger.warn(s"No supported document files found in: $documentsPath")
      logger.info(s"Supported formats: ${supportedExtensions.mkString(", ")}")
      return
    }

    documentsTotal = documentFiles.size
    ingesting = true
    documentsProcessed = 0
    logger.info(s"Found ${documentFiles.size} document files - starting async ingestion")

    implicit val ec = system.dispatcher

    Source(documentFiles)
      .mapAsync(1)(path => Future(processDocument(path)))
      .runWith(Sink.foreach { doc =>
        indexedDocuments = indexedDocuments :+ doc
        documentsProcessed += 1
        logger.info(s"Ingestion progress: $documentsProcessed/$documentsTotal - ${doc.name} (${doc.chunks} chunks)")
      })
      .onComplete {
        case scala.util.Success(_) =>
          ingesting = false
          val totalChunks = indexedDocuments.map(_.chunks).sum
          logger.info(s"Async ingestion complete: ${indexedDocuments.size} documents, $totalChunks total chunks")
        case scala.util.Failure(ex) =>
          ingesting = false
          logger.error(s"Async ingestion failed: ${ex.getMessage}", ex)
      }
  }

  private def createAssistant(): ChatAssistant = {
    val queryRouter = new DefaultQueryRouter(contentRetriever)

    val retrievalAugmentorBuilder = DefaultRetrievalAugmentor.builder()
      .queryRouter(queryRouter)

    if (rerankerConfig.enabled) {
      val cohereApiKey = sys.env.getOrElse("COHERE_API_KEY", "")

      if (cohereApiKey.isEmpty) {
        logger.warn("COHERE_API_KEY not set but reranking is enabled. Reranking will be disabled.")
        rerankerConfig = rerankerConfig.copy(enabled = false)
        val wrapper = LoggingContentAggregator(new DefaultContentAggregator())
        loggingAggregator = wrapper
        retrievalAugmentorBuilder.contentAggregator(wrapper)
      } else {
        logger.info(s"Enabling Cohere reranking with model: ${rerankerConfig.modelName}, minScore: ${rerankerConfig.minScore}")

        val scoringModel = CohereScoringModel.builder()
          .apiKey(cohereApiKey)
          .modelName(rerankerConfig.modelName)
          .logRequests(true)
          .logResponses(true)
          .build()

        val reRankingAggregator = ReRankingContentAggregator.builder()
          .scoringModel(scoringModel)
          .minScore(rerankerConfig.minScore)
          .build()

        val wrapper = LoggingContentAggregator(reRankingAggregator, reranking = true)
        loggingAggregator = wrapper
        retrievalAugmentorBuilder.contentAggregator(wrapper)
      }
    } else {
      val wrapper = LoggingContentAggregator(new DefaultContentAggregator())
      loggingAggregator = wrapper
      retrievalAugmentorBuilder.contentAggregator(wrapper)
    }

    val retrievalAugmentor = retrievalAugmentorBuilder.build()

    val model = OpenAiChatModel.builder()
      .apiKey(openAiApiKey)
      .modelName(GPT_4_O_MINI)
      .temperature(0.2)
      .timeout(java.time.Duration.ofSeconds(30))
      .logRequests(true)
      .build()

    val memoryProvider: ChatMemoryProvider = (memoryId: Object) => {
      sessionMemories.computeIfAbsent(memoryId, _ =>
        new DedupingChatMemory(MessageWindowChatMemory.withMaxMessages(10))
      )
    }

    AiServices.builder(classOf[ChatAssistant])
      .chatModel(model)
      .systemMessageProvider(_ => RagEngine.SystemPrompt)
      .retrievalAugmentor(retrievalAugmentor)
      .chatMemoryProvider(memoryProvider)
      .build()
  }
}

object RagEngine {
  private val SystemPrompt: String =
    """You are a knowledgeable assistant with access to a pdf document database.
      |Answer questions using ONLY the provided context.
      |If the answer cannot be found in the context, clearly state: "I don't have information about this."
      |However, when the retrieved context clearly relates to the question's topic or contains matching terms or names,
      |ALWAYS summarize what you found. Example response for this case: "I don't have exact information about this, however based on the retrieved content..."
      |
      |Guidelines:
      |- Be concise and direct
      |- For biographical or factual questions, prioritize accurate information from the sources
      |- If context is unclear or contradictory, note this explicitly
      |- Do not make assumptions beyond what the context provides""".stripMargin

  /**
    * Create RagEngine with custom configuration.
    * All parameters are optional and use constructor defaults if not specified.
    *
    * Examples:
    * - RagEngine(dropTableFirst = false)
    * - RagEngine(documentsPath = "/custom/path")
    * - RagEngine(dbHost = "prod-db.example.com", dropTableFirst = false)
    */
  def apply(
             documentsPath: String = sys.env.getOrElse("DOCUMENTS_PATH", "src/main/resources/content"),
             dbHost: String = "localhost",
             dbPort: Int = 5432,
             dbName: String = "ragdb",
             dbUser: String = "postgres",
             dbPassword: String = "postgres",
             dropTableFirst: Boolean = true,
             rerankerConfig: RerankerConfig = RerankerConfig()
           )(implicit system: ActorSystem): RagEngine = new RagEngine(
    documentsPath = documentsPath,
    dbHost = dbHost,
    dbPort = dbPort,
    dbName = dbName,
    dbUser = dbUser,
    dbPassword = dbPassword,
    dropTableFirst = dropTableFirst,
    initialRerankerConfig = rerankerConfig
  )
}

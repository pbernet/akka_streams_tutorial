package rag.mcp

import com.fasterxml.jackson.databind.ObjectMapper
import io.modelcontextprotocol.json.jackson2.JacksonMcpJsonMapper
import io.modelcontextprotocol.server.{McpServer, McpStatelessServerFeatures}
import io.modelcontextprotocol.spec.McpSchema.{CallToolResult, ServerCapabilities, Tool}
import org.apache.pekko.actor.ActorSystem
import org.slf4j.{Logger, LoggerFactory}
import rag.core.{RagEngine, RagEngineProvider}

import java.util.UUID
import scala.sys.process.*
import scala.util.{Failure, Success, Try}

/**
  * Model Context Protocol (MCP) server exposing RAG as MCP tools.
  *
  * API Surface (MCP Tools):
  * - query_rag: Query the RAG system with indexed documents and get answer with source attribution
  * - prepare_query: Prepare a query with retrieved chunks without calling the LLM
  * - list_documents: List all indexed PDF documents with chunk counts
  * - get_status: Get RAG system status
  * - configure_reranker: Adjust semantic reranking settings
  *
  * MCP Client Configuration Example:
  * {
  * "mcpServers": {
  * "local-rag": {
  * "url": "http://localhost:8091/mcp",
  * "transport": "streamable-http"
  * }
  * }
  * }
  */
object LocalRagMcpServer {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val jsonMapper = new JacksonMcpJsonMapper(new ObjectMapper())

  /**
    * Start the MCP Server.
    *
    * @param ragEngine       RagEngine instance providing RAG functionality
    * @param launchInspector Whether to auto-launch the MCP Inspector client (default: true)
    * @param system          ActorSystem for HTTP server and async operations
    */
  def start(ragEngine: RagEngine, launchInspector: Boolean = true)(implicit system: ActorSystem): Unit = {
    logger.info("Starting Local RAG MCP Server (HTTP transport)...")

    val transportProvider = PekkoHttpMcpTransport()
    val server = McpServer.sync(transportProvider)
      .serverInfo("local-rag-chat", "1.0.0")
      .capabilities(ServerCapabilities.builder()
        .tools(true)
        .build())
      .build()

    server.addTool(createQueryRagTool(ragEngine))
    server.addTool(createPrepareQueryTool(ragEngine))
    server.addTool(createListDocsTool(ragEngine))
    server.addTool(createGetStatusTool(ragEngine))
    server.addTool(createConfigureRerankerTool(ragEngine))

    logger.info("MCP Server initialized with tools: query_rag, prepare_query, list_documents, get_status, configure_reranker")
    logger.info("Listening at http://localhost:8091/mcp")

    if (launchInspector) {
      launchMcpInspector()
    }
  }

  /**
    * Launch the MCP Inspector CLI tool for testing and debugging MCP tools.
    *
    * Attempts to run `npx @modelcontextprotocol/inspector` in the background.
    * Gracefully handles failure if npm/npx is not available.
    */
  private def launchMcpInspector(): Unit = {
    val inspectorUrl = "http://localhost:8091/mcp"
    val inspectorCommand = s"npx @modelcontextprotocol/inspector@latest $inspectorUrl"
    logger.info(s"Launching MCP Inspector...")

    Try {
      val os = System.getProperty("os.name").toLowerCase

      if (os.contains("mac os x")) {
        Process(Seq("sh", "-c", inspectorCommand)).run()
        logger.info("MCP Inspector launched in background (macOS)")
      } else if (os.contains("windows")) {
        Seq("cmd", "/c", "start", "cmd", "/k", inspectorCommand).run()
        logger.info("MCP Inspector launched in new terminal (Windows)")
      } else {
        Process(Seq("sh", "-c", inspectorCommand)).run()
        logger.info("MCP Inspector launched in background (Linux)")
      }
    } match {
      case Success(_) =>
        logger.info("MCP Inspector started successfully")
      case Failure(ex) =>
        logger.warn(s"Could not auto-launch MCP Inspector: ${ex.getMessage}")
        logger.info(s"Please run manually: $inspectorCommand")
    }
  }

  /**
    * Create the 'query_rag' MCP tool.
    *
    * Accepts a natural language query and returns the LLM answer with source attribution.
    * This is the primary tool for RAG-powered question answering via MCP clients.
    *
    * @param ragEngine RagEngine instance to delegate RAG operations to
    * @return MCP SyncToolSpecification for the query_rag tool
    */
  private def createQueryRagTool(ragEngine: RagEngine): McpStatelessServerFeatures.SyncToolSpecification = {
    val schema =
      """
        |{
        |  "type": "object",
        |  "properties": {
        |    "query": {
        |      "type": "string",
        |      "description": "The question to ask the RAG system about the indexed PDF documents"
        |    }
        |  },
        |  "required": ["query"]
        |}
        |""".stripMargin

    val tool = Tool.builder()
      .name("query_rag")
      .description("Query the local RAG system with indexed PDF documents. Returns the answer along with source attribution showing which documents and chunks were used to generate the response.")
      .inputSchema(jsonMapper, schema)
      .build()

    McpStatelessServerFeatures.SyncToolSpecification.builder()
      .tool(tool)
      .callHandler((context, request) => {
        Try {
          val query = request.arguments().get("query").asInstanceOf[String]

          val sessionId = Option(context.get("sessionId")).map(_.toString).getOrElse("mcp-" + UUID.randomUUID().toString)
          logger.info(s"MCP query_rag called with: $query (session: $sessionId)")
          val richResponse = ragEngine.chatWithMetadata(query, sessionId)
          logger.info(s"RAG response generated (${richResponse.answer.length} chars) with ${richResponse.sources.length} sources")

          richResponse
        } match {
          case Success(richResponse) =>
            val sourcesText = if (richResponse.sources.nonEmpty) {
              val sourceLines = richResponse.sources.zipWithIndex.map { case (src, idx) =>
                val author = src.author.getOrElse("Unknown")
                val title = src.title.map(t => s" - $t").getOrElse("")
                val preview = src.chunkText.take(100).trim.replace("\n", " ")
                f"${idx + 1}. ${src.fileName}$title (Author: $author, Pages: ${src.pageCount}, Relevance: ${src.similarity}%.2f)\n   Preview: $preview..."
              }
              s"\n\n## Sources Used\n${sourceLines.mkString("\n\n")}"
            } else {
              "\n\n## Sources Used\nNo sources found for this query."
            }

            CallToolResult.builder()
              .addTextContent(richResponse.answer + sourcesText)
              .isError(false)
              .build()
          case Failure(ex) =>
            logger.error(s"Error in query_rag: ${ex.getMessage}", ex)
            CallToolResult.builder()
              .addTextContent(s"Error: ${ex.getMessage}")
              .isError(true)
              .build()
        }
      })
      .build()
  }

  /**
    * Create the 'prepare_query' MCP tool.
    *
    * Retrieves matching chunks from the embedding store and assembles a complete prompt
    * ready for LLM processing. Does NOT call the LLM - allows MCP clients to use their own.
    *
    * @param ragEngine RagEngine instance to delegate retrieval operations to
    * @return MCP SyncToolSpecification for the prepare_query tool
    */
  private def createPrepareQueryTool(ragEngine: RagEngine): McpStatelessServerFeatures.SyncToolSpecification = {
    val schema =
      """
        |{
        |  "type": "object",
        |  "properties": {
        |    "query": {
        |      "type": "string",
        |      "description": "The question to prepare for LLM processing"
        |    }
        |  },
        |  "required": ["query"]
        |}
        |""".stripMargin

    val tool = Tool.builder()
      .name("prepare_query")
      .description("Prepare a query by retrieving matching chunks from indexed PDF documents and assembling a complete prompt. Returns the prepared query text with context and full source chunk details, without calling an LLM. Use this when you want to send the query to your own LLM.")
      .inputSchema(jsonMapper, schema)
      .build()

    McpStatelessServerFeatures.SyncToolSpecification.builder()
      .tool(tool)
      .callHandler((context, request) => {
        Try {
          val query = request.arguments().get("query").asInstanceOf[String]
          logger.info(s"MCP prepare_query called with: $query")

          val richResponse = ragEngine.prepareQuery(query)
          logger.info(s"Prepared query with ${richResponse.sources.length} source chunks")

          richResponse
        } match {
          case Success(richResponse) =>
            val sourcesJson = richResponse.sources.map { src =>
              s"""|  {
                  |    "fileName": "${src.fileName}",
                  |    "pageCount": ${src.pageCount},
                  |    "chunkIndex": ${src.chunkIndex},
                  |    "similarity": ${f"${src.similarity}%.4f"},
                  |    "title": ${src.title.map(t => s""""$t"""").getOrElse("null")},
                  |    "author": ${src.author.map(a => s""""$a"""").getOrElse("null")},
                  |    "creationDate": ${src.creationDate.map(d => s""""$d"""").getOrElse("null")},
                  |    "subject": ${src.subject.map(s => s""""$s"""").getOrElse("null")},
                  |    "chunkText": "${src.chunkText.replace("\"", "\\\"").replace("\n", "\\n")}"
                  |  }""".stripMargin
            }.mkString(",\n")

            val responseJson =
              s"""|{
                  |  "preparedQuery": "${richResponse.preparedQuery.getOrElse("").replace("\"", "\\\"").replace("\n", "\\n")}",
                  |  "totalChunksRetrieved": ${richResponse.totalChunksRetrieved},
                  |  "sources": [
                  |$sourcesJson
                  |  ]
                  |}""".stripMargin

            CallToolResult.builder()
              .addTextContent(responseJson)
              .isError(false)
              .build()
          case Failure(ex) =>
            logger.error(s"Error in prepare_query: ${ex.getMessage}", ex)
            CallToolResult.builder()
              .addTextContent(s"Error: ${ex.getMessage}")
              .isError(true)
              .build()
        }
      })
      .build()
  }

  /**
    * Create the 'list_documents' MCP tool.
    *
    * Lists all indexed PDF documents and their chunk counts. Takes no input parameters.
    *
    * @param ragEngine RagEngine instance to delegate document listing to
    * @return MCP SyncToolSpecification for the list_documents tool
    */
  private def createListDocsTool(ragEngine: RagEngine): McpStatelessServerFeatures.SyncToolSpecification = {
    val schema = """{"type": "object", "properties": {}}"""

    val tool = Tool.builder()
      .name("list_documents")
      .description("List all PDF documents that have been indexed in the RAG system, including the number of chunks per document.")
      .inputSchema(jsonMapper, schema)
      .build()

    McpStatelessServerFeatures.SyncToolSpecification.builder()
      .tool(tool)
      .callHandler((context, request) => {
        Try {
          logger.info("MCP list_documents called")
          val docs = ragEngine.getIndexedDocuments
          if (docs.isEmpty) "No documents indexed."
          else docs.map(d => s"- ${d.name}: ${d.chunks} chunks").mkString("\n")
        } match {
          case Success(result) =>
            CallToolResult.builder()
              .addTextContent(result)
              .isError(false)
              .build()
          case Failure(ex) =>
            logger.error(s"Error in list_documents: ${ex.getMessage}", ex)
            CallToolResult.builder()
              .addTextContent(s"Error: ${ex.getMessage}")
              .isError(true)
              .build()
        }
      })
      .build()
  }

  /**
    * Create the 'get_status' MCP tool.
    *
    * Returns the current status of the RAG system including document count, total chunks,
    * and readiness state. Takes no input parameters.
    *
    * @param ragEngine RagEngine instance to delegate status queries to
    * @return MCP SyncToolSpecification for the get_status tool
    */
  private def createGetStatusTool(ragEngine: RagEngine): McpStatelessServerFeatures.SyncToolSpecification = {
    val schema = """{"type": "object", "properties": {}}"""

    val tool = Tool.builder()
      .name("get_status")
      .description("Get the current status of the RAG system including document count, total chunks, and readiness state.")
      .inputSchema(jsonMapper, schema)
      .build()

    McpStatelessServerFeatures.SyncToolSpecification.builder()
      .tool(tool)
      .callHandler((context, request) => {
        Try {
          logger.info("MCP get_status called")
          val status = ragEngine.getStatus
          val rerankerInfo = if (status.rerankerEnabled)
            s"Cohere reranking enabled (model: ${status.rerankerModelName}, minScore: ${status.rerankerMinScore})"
          else
            "Reranking disabled"
          s"""Status: ${status.status}
             |Message: ${status.message}
             |Documents indexed: ${status.documentsIndexed}
             |Total chunks: ${status.totalChunks}
             |$rerankerInfo""".stripMargin
        } match {
          case Success(result) =>
            CallToolResult.builder()
              .addTextContent(result)
              .isError(false)
              .build()
          case Failure(ex) =>
            logger.error(s"Error in get_status: ${ex.getMessage}", ex)
            CallToolResult.builder()
              .addTextContent(s"Error: ${ex.getMessage}")
              .isError(true)
              .build()
        }
      })
      .build()
  }

  /**
    * Create the 'configure_reranker' MCP tool.
    *
    * Allows MCP clients to dynamically adjust semantic reranking settings: minimum relevance
    * score threshold and enabled/disabled state. Changes take effect immediately on the next query.
    *
    * @param ragEngine RagEngine instance to delegate configuration updates to
    * @return MCP SyncToolSpecification for the configure_reranker tool
    */
  private def createConfigureRerankerTool(ragEngine: RagEngine): McpStatelessServerFeatures.SyncToolSpecification = {
    val schema =
      """
        |{
        |  "type": "object",
        |  "properties": {
        |    "min_score": {
        |      "type": "number",
        |      "description": "Minimum relevance score threshold for reranking (0.0 to 1.0)",
        |      "minimum": 0.0,
        |      "maximum": 1.0,
        |      "examples": [0.3, 0.5, 0.7]
        |    },
        |    "enabled": {
        |      "type": "boolean",
        |      "description": "Enable or disable reranking (optional)",
        |      "examples": [true, false]
        |    }
        |  },
        |  "required": ["min_score"]
        |}
        |""".stripMargin

    val tool = Tool.builder()
      .name("configure_reranker")
      .description("Configure the reranking behavior: adjust minScore threshold (0-1) or toggle reranking enabled/disabled. Changes take effect immediately on next query.")
      .inputSchema(jsonMapper, schema)
      .build()

    McpStatelessServerFeatures.SyncToolSpecification.builder()
      .tool(tool)
      .callHandler((context, request) => {
        Try {
          val minScore = request.arguments().get("min_score").asInstanceOf[Number].doubleValue()
          val enabledOpt = Option(request.arguments().get("enabled")).map(_.asInstanceOf[Boolean])

          logger.info(s"MCP configure_reranker called: minScore=$minScore, enabled=$enabledOpt")
          ragEngine.setMinScore(minScore)
          enabledOpt.foreach(ragEngine.setRerankerEnabled)

          val currentConfig = ragEngine.getRerankerConfig
          f"Reranker configured successfully:\n- minScore: ${currentConfig.minScore}%.2f\n- enabled: ${currentConfig.enabled}"
        } match {
          case Success(message) =>
            logger.info(message)
            CallToolResult.builder()
              .addTextContent(message)
              .isError(false)
              .build()
          case Failure(ex) =>
            logger.error(s"Error in configure_reranker: ${ex.getMessage}", ex)
            CallToolResult.builder()
              .addTextContent(s"Error: ${ex.getMessage}")
              .isError(true)
              .build()
        }
      })
      .build()
  }

  /**
    * Main method for standalone execution.
    *
    * Retrieves or creates a shared RagEngine singleton and starts the MCP server.
    */
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("mcp-server")

    logger.info("Starting Local RAG MCP Server with shared RagEngine...")
    val ragEngine = RagEngineProvider.get()
    start(ragEngine)
  }
}

package rag.app

import org.apache.pekko.actor.ActorSystem
import org.slf4j.{Logger, LoggerFactory}
import rag.core.RagEngineProvider
import rag.mcp.LocalRagMcpServer

/**
  * Launcher for both RAG Chat and MCP Server
  * Runs both servers in the same JVM to share the [[rag.core.RagEngine]] singleton
  *
  * Endpoints:
  * - RAG Chat HTTP Server: http://localhost:8090/rag (Web UI + REST API)
  * - MCP Server: http://localhost:8091/mcp (Model Context Protocol for Claude Desktop, etc.)
  */
object LocalRagApplication extends App {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  logger.info("=" * 80)
  logger.info("Starting unified Local RAG Application...")
  logger.info("=" * 80)

  implicit val system: ActorSystem = ActorSystem("local-rag-unified")

  logger.info("Initializing shared RagEngine...")
  private val ragEngine = RagEngineProvider.get()
  logger.info("Shared RagEngine initialized")

  logger.info("")
  logger.info("Starting servers...")
  logger.info("-" * 80)

  LocalRagChat.start(ragEngine)

  LocalRagMcpServer.start(ragEngine)

  logger.info("-" * 80)
  logger.info("Both servers started successfully")
  logger.info("")
  logger.info("Available endpoints:")
  logger.info("  - RAG Chat UI:  http://localhost:8090/rag")
  logger.info("  - MCP Server:   http://localhost:8091/mcp")
  logger.info("=" * 80)
}

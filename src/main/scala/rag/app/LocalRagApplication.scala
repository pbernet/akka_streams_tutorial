package rag.app

import org.apache.pekko.actor.ActorSystem
import org.slf4j.{Logger, LoggerFactory}
import rag.core.RagEngineProvider
import rag.mcp.LocalRagMcpServer

/**
  * Launcher for the RAG Chat and MCP servers in the same JVM, sharing a single
  * [[rag.core.RagEngine]] instance.
  *
  *  - Chat UI / REST API: http://localhost:8090/rag
  *  - MCP server:         http://localhost:8091/mcp
  */
object LocalRagApplication extends App {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  implicit val system: ActorSystem = ActorSystem("local-rag-unified")
  private val ragEngine = RagEngineProvider.get()

  LocalRagChat.start(ragEngine)
  LocalRagMcpServer.start(ragEngine)
}

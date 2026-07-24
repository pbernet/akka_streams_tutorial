package rag.mcp

import com.fasterxml.jackson.databind.ObjectMapper
import io.modelcontextprotocol.common.McpTransportContext
import io.modelcontextprotocol.json.jackson2.JacksonMcpJsonMapper
import io.modelcontextprotocol.server.McpStatelessServerHandler
import io.modelcontextprotocol.spec.McpSchema.{JSONRPCRequest, JSONRPCResponse}
import io.modelcontextprotocol.spec.McpStatelessServerTransport
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.*
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.slf4j.{Logger, LoggerFactory}
import reactor.core.publisher.Mono

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * Pekko HTTP-based transport for the MCP (Model Context Protocol) Streamable HTTP specification.
  *
  * Bridges the Java MCP SDK's [[McpStatelessServerTransport]] interface with a Pekko HTTP server,
  * exposing a single HTTP endpoint that accepts JSON-RPC requests from MCP clients
  * (e.g. MCP Inspector, Claude Desktop, or custom MCP clients).
  *
  * Handles:
  *  - JSON-RPC request/response dispatch to the MCP handler
  *  - JSON-RPC notification acknowledgement (messages without an `id` field)
  *  - CORS preflight for browser-based MCP clients
  *  - Session tracking via `Mcp-Session-Id` header
  *  - A GET health-check endpoint
  *
  * @param host the hostname to bind to
  * @param port the port to listen on
  * @param path the URL path prefix for the MCP endpoint
  */
class PekkoHttpMcpTransport(
                             host: String = "localhost",
                             port: Int = 8091,
                             path: String = "mcp"
                           )(implicit system: ActorSystem) extends McpStatelessServerTransport {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val objectMapper = new ObjectMapper()
  private val jsonMapper = new JacksonMcpJsonMapper(objectMapper)

  private var mcpHandler: McpStatelessServerHandler = _
  private var bindingFuture: Future[Http.ServerBinding] = _

  implicit private val ec: ExecutionContext = system.dispatcher

  override def setMcpHandler(handler: McpStatelessServerHandler): Unit = {
    this.mcpHandler = handler
    startServer()
  }

  private def startServer(): Unit = {
    val routes = createRoutes()
    bindingFuture = Http().newServerAt(host, port).bind(routes)

    bindingFuture.onComplete {
      case Success(binding) =>
        logger.info(s"MCP HTTP Server started at: http://$host:$port/$path")
      case Failure(ex) =>
        logger.error(s"Failed to bind MCP HTTP server: ${ex.getMessage}", ex)
    }
  }

  private def createRoutes(): Route = {
    // CORS headers for browser-based clients like MCP Inspector
    val corsHeaders = List(
      `Access-Control-Allow-Origin`.*,
      `Access-Control-Allow-Methods`(HttpMethods.GET, HttpMethods.POST, HttpMethods.OPTIONS),
      `Access-Control-Allow-Headers`("Content-Type", "Authorization", "Accept", "Mcp-Session-Id"),
      `Access-Control-Expose-Headers`("Mcp-Session-Id"),
      `Access-Control-Max-Age`(86400)
    )

    respondWithHeaders(corsHeaders) {
      pathPrefix(path) {
        concat(
          options {
            // Handle CORS preflight requests
            complete(StatusCodes.OK)
          },
          post {
            optionalHeaderValueByName("Mcp-Session-Id") { mcpSessionId =>
              entity(as[String]) { requestBody =>
                handleMcpRequest(requestBody, mcpSessionId)
              }
            }
          },
          get {
            // Return server info for health checks
            complete(HttpEntity(
              ContentTypes.`application/json`,
              """{"status":"ok","server":"local-rag-mcp","transport":"streamable-http","version":"1.0.0"}"""
            ))
          }
        )
      }
    }
  }

  private def handleMcpRequest(requestBody: String, mcpSessionId: Option[String]): Route = {
    val sessionId = mcpSessionId.getOrElse("mcp-" + UUID.randomUUID().toString)
    logger.info(s"Received MCP request (session: $sessionId, length: ${requestBody.length})")

    if (mcpHandler == null) {
      complete(StatusCodes.ServiceUnavailable -> """{"error":"MCP handler not initialized"}""")
    } else {
      val context = McpTransportContext.create(java.util.Map.of("sessionId", sessionId))

      Try {
        if (requestBody.isEmpty) {
          throw new IllegalArgumentException("Request body is empty")
        }

        val jsonNode = objectMapper.readTree(requestBody)
        val hasId = jsonNode.has("id") && !jsonNode.get("id").isNull

        if (!hasId) {
          val method = if (jsonNode.has("method")) jsonNode.get("method").asText() else "unknown"
          logger.info(s"Received notification (no id): method=$method - acknowledging with 202")
          None
        } else {
          val jsonRpcRequest = jsonMapper.readValue(requestBody, classOf[JSONRPCRequest])
          logger.info(s"MCP request: method=${jsonRpcRequest.method()}, id=${jsonRpcRequest.id()}")

          val responseMono: Mono[JSONRPCResponse] = mcpHandler.handleRequest(context, jsonRpcRequest)
          val response = responseMono.block()

          val responseJson = jsonMapper.writeValueAsString(response)
          logger.info(s"MCP response for id=${jsonRpcRequest.id()}")
          Some(responseJson)
        }
      } match {
        case Success(Some(responseJson)) =>
          respondWithHeader(RawHeader("Mcp-Session-Id", sessionId)) {
            complete(HttpEntity(ContentTypes.`application/json`, responseJson))
          }
        case Success(None) =>
          respondWithHeader(RawHeader("Mcp-Session-Id", sessionId)) {
            complete(StatusCodes.Accepted)
          }
        case Failure(ex) =>
          logger.error(s"Error handling MCP request: ${ex.getMessage}", ex)
          val errorResponse = createErrorResponse(ex)
          complete(StatusCodes.InternalServerError -> HttpEntity(ContentTypes.`application/json`, errorResponse))
      }
    }
  }

  private def createErrorResponse(ex: Throwable): String = {
    s"""{"jsonrpc":"2.0","error":{"code":-32603,"message":"${ex.getMessage.replace("\"", "\\\"")}"},"id":null}"""
  }

  override def closeGracefully(): Mono[Void] = {
    Mono.fromRunnable(() => {
      if (bindingFuture != null) {
        bindingFuture.flatMap(_.unbind()).onComplete {
          case Success(_) => logger.info("MCP HTTP Server stopped")
          case Failure(ex) => logger.error(s"Error stopping MCP HTTP server: ${ex.getMessage}", ex)
        }
      }
    })
  }

  override def close(): Unit = {
    closeGracefully().block()
  }
}

object PekkoHttpMcpTransport {
  def apply(host: String = "localhost", port: Int = 8091, path: String = "mcp")
           (implicit system: ActorSystem): PekkoHttpMcpTransport = {
    new PekkoHttpMcpTransport(host, port, path)
  }
}

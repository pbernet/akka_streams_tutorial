package rag.mcp

import com.fasterxml.jackson.databind.ObjectMapper
import io.modelcontextprotocol.common.McpTransportContext
import io.modelcontextprotocol.json.jackson2.JacksonMcpJsonMapper
import io.modelcontextprotocol.server.McpStatelessServerHandler
import io.modelcontextprotocol.spec.McpSchema.{ErrorCodes, JSONRPCNotification, JSONRPCRequest, JSONRPCResponse}
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
import scala.jdk.FutureConverters.*
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
  *  - A separate GET health-check endpoint at `/health/{endpointPath}`
  *
  * @param host the hostname to bind to
  * @param port the port to listen on
  * @param endpointPath the URL path for the MCP endpoint
  * @param allowedOrigins browser origins allowed to call the endpoint
  */
class PekkoHttpMcpTransport(
                             host: String = "localhost",
                             port: Int = 8091,
                             endpointPath: String = "mcp",
                             allowedOrigins: Set[String] = Set("http://localhost:6274", "http://127.0.0.1:6274")
                           )(implicit system: ActorSystem) extends McpStatelessServerTransport {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val objectMapper = new ObjectMapper()
  private val jsonMapper = new JacksonMcpJsonMapper(objectMapper)

  @volatile private var bindingFuture: Option[Future[Http.ServerBinding]] = None

  implicit private val ec: ExecutionContext = system.dispatcher

  override def setMcpHandler(handler: McpStatelessServerHandler): Unit = synchronized {
    require(handler != null, "MCP handler must not be null")
    if (bindingFuture.nonEmpty) {
      throw new IllegalStateException("MCP HTTP server has already been started")
    }
    startServer(handler)
  }

  private def startServer(handler: McpStatelessServerHandler): Unit = {
    val routes = createRoutes(handler)
    val binding = Http().newServerAt(host, port).bind(routes)
    bindingFuture = Some(binding)

    binding.onComplete {
      case Success(binding) =>
        logger.info(s"MCP HTTP Server started at: http://$host:$port/$endpointPath")
      case Failure(ex) =>
        logger.error(s"Failed to bind MCP HTTP server: ${ex.getMessage}", ex)
    }
  }

  private[mcp] def createRoutes(handler: McpStatelessServerHandler): Route = {
    optionalHeaderValueByName("Origin") { origin =>
      if (origin.exists(value => !allowedOrigins.contains(value))) {
        complete(StatusCodes.Forbidden -> "Origin is not allowed")
      } else {
        respondWithHeaders(corsHeaders(origin)) {
          concat(
            path(endpointPath) {
              concat(
                options {
                  complete(StatusCodes.NoContent)
                },
                post {
                  extractRequest { request =>
                    validateHttpHeaders(request) match {
                      case Some(response) => complete(response)
                      case None =>
                        optionalHeaderValueByName("Mcp-Session-Id") { mcpSessionId =>
                          entity(as[String]) { requestBody =>
                            handleMcpRequest(handler, requestBody, mcpSessionId)
                          }
                        }
                    }
                  }
                },
                get {
                  complete(StatusCodes.MethodNotAllowed)
                }
              )
            },
            pathPrefix("health") {
              path(endpointPath) {
                get {
                  complete(HttpEntity(
                    ContentTypes.`application/json`,
                    """{"status":"ok","server":"local-rag-mcp","transport":"streamable-http","version":"1.0.0"}"""
                  ))
                }
              }
            }
          )
        }
      }
    }
  }

  private def corsHeaders(origin: Option[String]): List[HttpHeader] = {
    val commonHeaders: List[HttpHeader] = List(
      `Access-Control-Allow-Methods`(HttpMethods.GET, HttpMethods.POST, HttpMethods.OPTIONS),
      `Access-Control-Allow-Headers`("Content-Type", "Authorization", "Accept", "Mcp-Session-Id", "MCP-Protocol-Version"),
      `Access-Control-Expose-Headers`("Mcp-Session-Id"),
      `Access-Control-Max-Age`(86400)
    )
    origin.map(value => RawHeader("Access-Control-Allow-Origin", value)).toList ++ commonHeaders
  }

  private def validateHttpHeaders(request: HttpRequest): Option[HttpResponse] = {
    val isJson = request.entity.contentType.mediaType == MediaTypes.`application/json`
    val acceptedMediaRanges = request.headers.collect { case accept: Accept => accept.mediaRanges }.flatten
    val acceptsRequiredTypes = Seq(MediaTypes.`application/json`, MediaTypes.`text/event-stream`).forall { mediaType =>
      acceptedMediaRanges.exists(mediaRange => mediaRange.qValue > 0.0f && mediaRange.matches(mediaType))
    }
    val requestedVersion = request.headers.find(_.is("mcp-protocol-version")).map(_.value())
    val supportedVersion = requestedVersion.forall(protocolVersions().contains)

    if (!isJson) {
      Some(errorResponse(StatusCodes.UnsupportedMediaType, ErrorCodes.INVALID_REQUEST, "Content-Type must be application/json"))
    } else if (!acceptsRequiredTypes) {
      Some(errorResponse(StatusCodes.NotAcceptable, ErrorCodes.INVALID_REQUEST, "Accept must include application/json and text/event-stream"))
    } else if (!supportedVersion) {
      Some(errorResponse(StatusCodes.BadRequest, ErrorCodes.INVALID_REQUEST, s"Unsupported MCP protocol version: ${requestedVersion.get}"))
    } else {
      None
    }
  }

  private def handleMcpRequest(
                                handler: McpStatelessServerHandler,
                                requestBody: String,
                                mcpSessionId: Option[String]
                              ): Route = {
    val sessionId = mcpSessionId.getOrElse("mcp-" + UUID.randomUUID().toString)
    logger.info(s"Received MCP request (session: $sessionId, length: ${requestBody.length})")
    val context = McpTransportContext.create(java.util.Map.of("sessionId", sessionId))

    Try {
      if (requestBody.isEmpty) {
        throw new IllegalArgumentException("Request body is empty")
      }

      val jsonNode = objectMapper.readTree(requestBody)
      if (!jsonNode.isObject || !jsonNode.hasNonNull("method") || !jsonNode.hasNonNull("jsonrpc") || jsonNode.get("jsonrpc").asText() != "2.0") {
        throw new IllegalArgumentException("JSON-RPC message must be an object with jsonrpc 2.0 and a method")
      }
      val hasId = jsonNode.has("id")

      if (hasId) {
        Left(Try(jsonMapper.readValue(requestBody, classOf[JSONRPCRequest]))
          .recover { case ex => throw new IllegalArgumentException("Invalid JSON-RPC request", ex) }
          .get)
      } else {
        Right(Try(jsonMapper.readValue(requestBody, classOf[JSONRPCNotification]))
          .recover { case ex => throw new IllegalArgumentException("Invalid JSON-RPC notification", ex) }
          .get)
      }
    } match {
      case Success(Left(jsonRpcRequest)) =>
        logger.info(s"MCP request: method=${jsonRpcRequest.method()}, id=${jsonRpcRequest.id()}")
        val responseFuture = Mono.defer(() => handler.handleRequest(context, jsonRpcRequest)).toFuture.asScala
          .map { (response: JSONRPCResponse) =>
            logger.info(s"MCP response for id=${jsonRpcRequest.id()}")
            responseWithSession(
              StatusCodes.OK,
              sessionId,
              HttpEntity(ContentTypes.`application/json`, jsonMapper.writeValueAsString(response))
            )
          }
          .recover { case ex =>
            logger.error(s"Error handling MCP request: ${ex.getMessage}", ex)
            errorResponse(StatusCodes.InternalServerError, ErrorCodes.INTERNAL_ERROR, Option(ex.getMessage).getOrElse("Internal error"))
          }
        complete(responseFuture)

      case Success(Right(notification)) =>
        logger.info(s"MCP notification: method=${notification.method()}")
        val acknowledgementFuture = Mono.defer(() => handler.handleNotification(context, notification)).toFuture.asScala
          .map(_ => responseWithSession(StatusCodes.Accepted, sessionId, HttpEntity.Empty))
          .recover { case ex =>
            logger.error(s"Error handling MCP notification: ${ex.getMessage}", ex)
            errorResponse(StatusCodes.InternalServerError, ErrorCodes.INTERNAL_ERROR, Option(ex.getMessage).getOrElse("Internal error"))
          }
        complete(acknowledgementFuture)

      case Failure(ex) =>
        logger.warn(s"Invalid MCP message: ${ex.getMessage}")
        val errorCode = if (ex.isInstanceOf[com.fasterxml.jackson.core.JsonProcessingException]) ErrorCodes.PARSE_ERROR else ErrorCodes.INVALID_REQUEST
        complete(errorResponse(StatusCodes.BadRequest, errorCode, Option(ex.getMessage).getOrElse("Invalid JSON-RPC message")))
    }
  }

  private def responseWithSession(status: StatusCode, sessionId: String, entity: ResponseEntity): HttpResponse = {
    HttpResponse(status, headers = List(RawHeader("Mcp-Session-Id", sessionId)), entity = entity)
  }

  private def errorResponse(status: StatusCode, code: Int, message: String): HttpResponse = {
    val response = objectMapper.createObjectNode()
    response.put("jsonrpc", "2.0")
    val error = response.putObject("error")
    error.put("code", code)
    error.put("message", message)
    response.putNull("id")
    HttpResponse(status, entity = HttpEntity(ContentTypes.`application/json`, objectMapper.writeValueAsString(response)))
  }

  override def closeGracefully(): Mono[Void] = {
    Mono.defer(() => bindingFuture match {
      case Some(binding) =>
        Mono.fromCompletionStage(binding.flatMap(_.unbind()).asJava)
          .doOnSuccess(_ => logger.info("MCP HTTP Server stopped"))
          .doOnError(ex => logger.error(s"Error stopping MCP HTTP server: ${ex.getMessage}", ex))
          .`then`()
      case None =>
        Mono.empty()
    })
  }

  override def protocolVersions(): java.util.List[String] = {
    java.util.List.of("2025-03-26", "2025-06-18", "2025-11-25")
  }
}

object PekkoHttpMcpTransport {
  def apply(
             host: String = "localhost",
             port: Int = 8091,
             endpointPath: String = "mcp",
             allowedOrigins: Set[String] = Set("http://localhost:6274", "http://127.0.0.1:6274")
           )
           (implicit system: ActorSystem): PekkoHttpMcpTransport = {
    new PekkoHttpMcpTransport(host, port, endpointPath, allowedOrigins)
  }
}

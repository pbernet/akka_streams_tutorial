package rag.mcp

import io.modelcontextprotocol.common.McpTransportContext
import io.modelcontextprotocol.server.McpStatelessServerHandler
import io.modelcontextprotocol.spec.McpSchema.{ErrorCodes, JSONRPCNotification, JSONRPCRequest, JSONRPCResponse}
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.{Accept, RawHeader}
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import reactor.core.publisher.Mono

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{CompletableFuture, Executors, TimeUnit}
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

class PekkoHttpMcpTransportSpec extends AnyWordSpec with Matchers with ScalatestRouteTest {

  private val requestMethod = new AtomicReference[String]()
  private val notificationMethod = new AtomicReference[String]()
  private val receivedSessionId = new AtomicReference[String]()

  private val handler: McpStatelessServerHandler = new McpStatelessServerHandler {
    override def handleRequest(context: McpTransportContext, request: JSONRPCRequest): Mono[JSONRPCResponse] = {
      requestMethod.set(request.method())
      receivedSessionId.set(context.get("sessionId").toString)
      Mono.just(JSONRPCResponse.result(request.id(), java.util.Map.of("ok", java.lang.Boolean.TRUE)))
    }

    override def handleNotification(context: McpTransportContext, notification: JSONRPCNotification): Mono[Void] = {
      notificationMethod.set(notification.method())
      receivedSessionId.set(context.get("sessionId").toString)
      Mono.empty()
    }
  }

  private val transport = new PekkoHttpMcpTransport()(using system)
  private val route = transport.createRoutes(handler)
  private val requiredHeaders: List[HttpHeader] = List(
    Accept(MediaTypes.`application/json`, MediaTypes.`text/event-stream`),
    RawHeader("MCP-Protocol-Version", "2025-11-25")
  )

  private def post(body: String, headers: List[HttpHeader] = requiredHeaders): HttpRequest = {
    HttpRequest(
      method = HttpMethods.POST,
      uri = "/mcp",
      headers = headers,
      entity = HttpEntity(ContentTypes.`application/json`, body)
    )
  }

  "PekkoHttpMcpTransport" should {
    "dispatch requests and preserve the supplied session id" in {
      requestMethod.set(null)
      receivedSessionId.set(null)
      val headers = RawHeader("Mcp-Session-Id", "conversation-1") :: requiredHeaders

      post("""{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}""", headers) ~> route ~> check {
        status shouldBe StatusCodes.OK
        responseAs[String] should include("\"ok\":true")
        header("Mcp-Session-Id").map(_.value()) shouldBe Some("conversation-1")
      }

      requestMethod.get() shouldBe "tools/list"
      receivedSessionId.get() shouldBe "conversation-1"
    }

    "return control without blocking on an incomplete handler response" in {
      val handlerCompletion = new CompletableFuture[JSONRPCResponse]()
      val response = JSONRPCResponse.result(Integer.valueOf(42), java.util.Map.of("ok", java.lang.Boolean.TRUE))
      val delayedHandler = new McpStatelessServerHandler {
        override def handleRequest(context: McpTransportContext, request: JSONRPCRequest): Mono[JSONRPCResponse] = {
          Mono.fromFuture(handlerCompletion)
        }

        override def handleNotification(context: McpTransportContext, notification: JSONRPCNotification): Mono[Void] = {
          Mono.empty()
        }
      }
      val delayedRoute = transport.createRoutes(delayedHandler)
      val routeHandler = Route.toFunction(delayedRoute)
      val invocationExecutor = Executors.newSingleThreadExecutor()
      val invocationResult = new CompletableFuture[Future[HttpResponse]]()

      try {
        invocationExecutor.execute(() => {
          try {
            invocationResult.complete(routeHandler(post(
              """{"jsonrpc":"2.0","id":42,"method":"tools/list","params":{}}"""
            )))
          } catch {
            case ex: Throwable => invocationResult.completeExceptionally(ex)
          }
        })

        val responseFuture = invocationResult.get(1, TimeUnit.SECONDS)
        responseFuture.isCompleted shouldBe false

        handlerCompletion.complete(response)
        val httpResponse = Await.result(responseFuture, 3.seconds)
        httpResponse.status shouldBe StatusCodes.OK
      } finally {
        handlerCompletion.complete(response)
        invocationExecutor.shutdownNow()
      }
    }

    "dispatch notifications before acknowledging them" in {
      notificationMethod.set(null)

      post("""{"jsonrpc":"2.0","method":"notifications/initialized"}""") ~> route ~> check {
        status shouldBe StatusCodes.Accepted
        header("Mcp-Session-Id").map(_.value()) should not be empty
      }

      notificationMethod.get() shouldBe "notifications/initialized"
      receivedSessionId.get() should startWith("mcp-")
    }

    "reject unsupported protocol versions" in {
      val headers: List[HttpHeader] = List(
        Accept(MediaTypes.`application/json`, MediaTypes.`text/event-stream`),
        RawHeader("MCP-Protocol-Version", "2026-07-28")
      )

      post("""{"jsonrpc":"2.0","id":1,"method":"tools/list"}""", headers) ~> route ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[String] should include("Unsupported MCP protocol version")
      }
    }

    "require the Streamable HTTP response media types" in {
      post("""{"jsonrpc":"2.0","id":1,"method":"tools/list"}""", Nil) ~> route ~> check {
        status shouldBe StatusCodes.NotAcceptable
      }
    }

    "validate accepted response types using parsed media ranges" in {
      val protocolVersion = RawHeader("MCP-Protocol-Version", "2025-11-25")
      val nearMatches = Accept(
        MediaType.applicationWithOpenCharset("jsonp"),
        MediaType.text("event-streaming")
      )
      val disabledJson = Accept(
        MediaRange(MediaTypes.`application/json`, 0.0f),
        MediaTypes.`text/event-stream`
      )

      post("""{"jsonrpc":"2.0","id":1,"method":"tools/list"}""", List(nearMatches, protocolVersion)) ~> route ~> check {
        status shouldBe StatusCodes.NotAcceptable
      }

      post("""{"jsonrpc":"2.0","id":1,"method":"tools/list"}""", List(disabledJson, protocolVersion)) ~> route ~> check {
        status shouldBe StatusCodes.NotAcceptable
      }

      post("""{"jsonrpc":"2.0","id":1,"method":"tools/list"}""", List(Accept(MediaRanges.`*/*`), protocolVersion)) ~> route ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    "return a JSON-RPC parse error for malformed JSON" in {
      post("{") ~> route ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[String] should include(s"\"code\":${ErrorCodes.PARSE_ERROR}")
      }
    }

    "reserve GET on the MCP endpoint and expose a separate health endpoint" in {
      Get("/mcp") ~> route ~> check {
        status shouldBe StatusCodes.MethodNotAllowed
      }

      Get("/health/mcp") ~> route ~> check {
        status shouldBe StatusCodes.OK
        responseAs[String] should include("\"status\":\"ok\"")
      }
    }

    "allow only configured browser origins" in {
      val allowed = HttpRequest(HttpMethods.OPTIONS, "/mcp", headers = List(RawHeader("Origin", "http://localhost:6274")))
      allowed ~> route ~> check {
        status shouldBe StatusCodes.NoContent
        header("Access-Control-Allow-Origin").map(_.value()) shouldBe Some("http://localhost:6274")
      }

      val forbidden = HttpRequest(HttpMethods.OPTIONS, "/mcp", headers = List(RawHeader("Origin", "https://example.com")))
      forbidden ~> route ~> check {
        status shouldBe StatusCodes.Forbidden
      }
    }
  }
}

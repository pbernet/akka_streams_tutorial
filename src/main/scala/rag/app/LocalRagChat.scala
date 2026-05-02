package rag.app

import io.circe.*
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.parser.*
import io.circe.syntax.*
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.HttpCookie
import org.apache.pekko.http.scaladsl.server.Directive1
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.slf4j.{Logger, LoggerFactory}
import rag.core.RagStatus.encoder as ragStatusEncoder
import rag.core.{RagEngine, RagEngineProvider, SourceSummary}

import java.nio.file.Paths
import java.util.UUID
import scala.sys.process.{Process, *}
import scala.util.{Failure, Success, Try}

/**
  * HTTP/REST API wrapper for the [[RagEngine]] with interactive web chat interface.
  *
  * API:
  * - GET  /rag                - Serve HTML chat interface
  * - POST /rag/chat           - Submit query and get answer with source attribution
  * - GET  /rag/status         - System status (documents indexed, total chunks)
  * - POST /rag/configure      - Update reranking settings (minScore, enabled/disabled)
  * - POST /rag/clear          - Clear chat history for new conversations
  */
object LocalRagChat {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  case class ChatRequest(query: String, sessionId: Option[String] = None)

  case class ChatResponse(
                           answer: String,
                           sources: List[SourceSummary],
                           sourceCount: Int
                         )

  case class ConfigureRequest(minScore: Double, enabled: Boolean)

  case class ConfigureResponse(success: Boolean, message: String, minScore: Double, enabled: Boolean)

  case class ClearChatResponse(success: Boolean, message: String)

  implicit val chatRequestDecoder: Decoder[ChatRequest] = deriveDecoder[ChatRequest]
  implicit val configureRequestDecoder: Decoder[ConfigureRequest] = deriveDecoder[ConfigureRequest]
  implicit val sourceSummaryEncoder: Encoder[SourceSummary] = deriveEncoder[SourceSummary]
  implicit val chatResponseEncoder: Encoder[ChatResponse] = deriveEncoder[ChatResponse]
  implicit val configureResponseEncoder: Encoder[ConfigureResponse] = deriveEncoder[ConfigureResponse]
  implicit val clearChatResponseEncoder: Encoder[ClearChatResponse] = deriveEncoder[ClearChatResponse]

  private val withSession: Directive1[String] =
    optionalCookie("rag-session").flatMap { maybe =>
      val sessionId = maybe.map(_.value).getOrElse("web-" + UUID.randomUUID().toString)
      setCookie(HttpCookie("rag-session", sessionId, path = Some("/rag"))).tflatMap { _ =>
        provide(sessionId)
      }
    }

  def start(ragEngine: RagEngine)(implicit system: ActorSystem): Unit = {
    import system.dispatcher

    logger.info("Starting Local RAG Chat HTTP server...")

    val routes =
      pathPrefix("rag") {
        concat(
          path("chat") {
            post {
              withSession { sessionId =>
                entity(as[String]) { jsonString =>
                  decode[ChatRequest](jsonString) match {
                    case Right(request) =>
                      logger.info(s"Received user query: ${request.query}")

                      Try {
                        val richResponse = ragEngine.chatWithMetadata(request.query, sessionId)

                        val sourceSummaries = richResponse.sources.map { src =>
                          SourceSummary(
                            fileName = src.fileName,
                            author = src.author,
                            score = src.score,
                            preview = src.chunkText.take(500).trim + (if (src.chunkText.length > 500) "..." else ""),
                            pageNumbers = src.pageNumbers,
                            reranked = src.reranked
                          )
                        }

                        val response = ChatResponse(
                          answer = richResponse.answer,
                          sources = sourceSummaries,
                          sourceCount = richResponse.sources.length
                        )

                        logger.info(s"Returning response with: ${response.sourceCount} sources")
                        HttpEntity(ContentTypes.`application/json`, response.asJson.noSpaces)
                      } match {
                        case Success(entity) =>
                          complete(entity)
                        case Failure(ex) =>
                          logger.error(s"Error processing chat request: ${ex.getMessage}", ex)
                          complete(StatusCodes.InternalServerError -> s"""{"error": "${ex.getMessage}"}""")
                      }

                    case Left(error) =>
                      complete(StatusCodes.BadRequest -> s"""{"error": "Invalid JSON: $error"}""")
                  }
                }
              }
            }
          },
          path("configure") {
            post {
              entity(as[String]) { jsonString =>
                decode[ConfigureRequest](jsonString) match {
                  case Right(request) =>
                    Try {
                      ragEngine.setMinScore(request.minScore)
                      ragEngine.setRerankerEnabled(request.enabled)

                      logger.info(s"Configuration updated: minScore=${request.minScore}, enabled=${request.enabled}")

                      val response = ConfigureResponse(
                        success = true,
                        message = s"Reranker configured successfully:\n- minScore: ${String.format("%.2f", request.minScore)}\n- enabled: ${request.enabled}",
                        minScore = request.minScore,
                        enabled = request.enabled
                      )

                      HttpEntity(ContentTypes.`application/json`, response.asJson.noSpaces)
                    } match {
                      case Success(entity) =>
                        complete(entity)
                      case Failure(ex) =>
                        logger.error(s"Error processing configure request: ${ex.getMessage}", ex)
                        val currentConfig = ragEngine.getRerankerConfig
                        val errorResponse = ConfigureResponse(
                          success = false,
                          message = s"Error: ${ex.getMessage}",
                          minScore = currentConfig.minScore,
                          enabled = currentConfig.enabled
                        )
                        complete(StatusCodes.BadRequest -> HttpEntity(ContentTypes.`application/json`, errorResponse.asJson.noSpaces))
                    }

                  case Left(error) =>
                    complete(StatusCodes.BadRequest -> s"""{"error": "Invalid JSON: $error"}""")
                }
              }
            }
          },
          path("clear") {
            post {
              withSession { sessionId =>
                Try {
                  ragEngine.clearChatMemory(sessionId)
                  logger.info(s"Chat memory cleared by user (session: $sessionId)")
                  val response = ClearChatResponse(success = true, message = "Chat history cleared successfully")
                  HttpEntity(ContentTypes.`application/json`, response.asJson.noSpaces)
                } match {
                  case Success(entity) =>
                    complete(entity)
                  case Failure(ex) =>
                    logger.error(s"Error clearing chat: ${ex.getMessage}", ex)
                    val errorResponse = ClearChatResponse(success = false, message = s"Error: ${ex.getMessage}")
                    complete(StatusCodes.InternalServerError -> HttpEntity(ContentTypes.`application/json`, errorResponse.asJson.noSpaces))
                }
              }
            }
          },
          path("status") {
            get {
              complete(HttpEntity(ContentTypes.`application/json`, ragEngine.getStatus.asJson.noSpaces))
            }
          },
          path("pdf" / Segment) { fileName =>
            get {
              val docsPath = sys.env.getOrElse("DOCUMENTS_PATH", "src/main/resources/content")
              val pdfFile = Paths.get(docsPath).resolve(fileName).toFile
              if (pdfFile.exists() && pdfFile.getName.toLowerCase.endsWith(".pdf")) {
                getFromFile(pdfFile, MediaTypes.`application/pdf`)
              } else {
                complete(StatusCodes.NotFound -> s"""{"error": "PDF not found: $fileName"}""")
              }
            }
          },
          pathEndOrSingleSlash {
            get {
              withSession { _ =>
                getFromFile(Paths.get("src/main/resources/ragchat.html").toFile, ContentTypes.`text/html(UTF-8)`)
              }
            }
          }
        )
      }

    Http().newServerAt("localhost", 8090).bind(routes).onComplete {
      case Success(_) =>
        logger.info(s"RAG Chat server started at http://localhost:8090/rag")

        val os = System.getProperty("os.name").toLowerCase
        val url = "http://localhost:8090/rag"
        if (os.contains("mac")) Process(s"open $url").!
        else if (os.startsWith("windows")) Seq("cmd", "/c", s"start $url").!
        else logger.info(s"Please open a browser at: $url")

      case Failure(ex) =>
        logger.error(s"Failed to bind HTTP server: ${ex.getMessage}", ex)
        system.terminate()
    }
  }

  /**
    * Main method for standalone execution.
    *
    * Retrieves or creates a shared RagEngine singleton and starts the HTTP server.
    */
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("rag-chat")

    logger.info("Starting Local RAG Chat application with shared RagEngine...")
    val ragEngine = RagEngineProvider.get()
    start(ragEngine)
  }
}

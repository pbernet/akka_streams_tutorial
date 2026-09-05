package rag.core

import ai.docling.serve.api.DoclingServeApi
import dev.langchain4j.data.document.Metadata
import dev.langchain4j.data.segment.TextSegment
import org.slf4j.LoggerFactory

import java.nio.file.Path
import java.time.Duration
import scala.jdk.CollectionConverters.*
import scala.util.Try

case class DoclingChunk(
                         text: String,
                         headings: List[String],
                         captions: List[String],
                         pageNumbers: List[Int],
                         filename: String,
                         chunkIndex: Int
                       ) {
  def toTextSegment: TextSegment = {
    val metadata = new Metadata()
    metadata.put("fileName", filename)
    metadata.put("chunkIndex", chunkIndex.toString)
    if (headings.nonEmpty) metadata.put("headings", headings.mkString(" > "))
    if (captions.nonEmpty) metadata.put("captions", captions.mkString(", "))
    if (pageNumbers.nonEmpty) metadata.put("pageNumbers", pageNumbers.mkString(","))
    metadata.put("producer", "Docling")
    TextSegment.from(text, metadata)
  }
}

object DoclingChunkingService {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val doclingServerUrl = sys.env.getOrElse("DOCLING_SERVER_URL", "http://localhost:5001")

  private lazy val client: DoclingServeApi = DoclingClientFactory.create(
    doclingServerUrl,
    Duration.ofMinutes(30),
    Duration.ofMinutes(30),
    Duration.ofSeconds(5)
  )

  def isAvailable: Boolean = {
    Try {
      val healthUrl = java.net.URI.create(s"$doclingServerUrl/health").toURL
      val connection = healthUrl.openConnection().asInstanceOf[java.net.HttpURLConnection]
      try {
        connection.setConnectTimeout(2000)
        connection.setReadTimeout(2000)
        connection.setRequestMethod("GET")
        connection.getResponseCode == 200
      } finally {
        connection.disconnect()
      }
    }.recover { case e: Exception =>
      logger.warn(s"Docling health check failed: ${e.getMessage}")
      false
    }.get
  }


  /**
    * Chunk document using Docling [[HierarchicalChunker]] with custom options.
    *
    * @param path              Path to the document file
    * @param useMarkdownTables Whether to preserve tables as markdown format
    * @return Try containing list of chunks or failure
    */
  private def chunkWithHierarchical(path: Path, useMarkdownTables: Boolean): Try[List[DoclingChunk]] = {
    val fileName = path.getFileName.toString
    logger.info(s"Chunking document via Docling HierarchicalChunker: $fileName (markdownTables=$useMarkdownTables)")

    Try {
      import ai.docling.serve.api.chunk.request.HierarchicalChunkDocumentRequest
      import ai.docling.serve.api.chunk.request.options.HierarchicalChunkerOptions
      import ai.docling.serve.api.convert.request.options.{ConvertDocumentOptions, PdfBackend}

      val convertOptions = ConvertDocumentOptions.builder()
        .doOcr(false)
        // DLPARSE_V4: Best text extraction quality (IBM custom parser based on qpdf)
        .pdfBackend(PdfBackend.DLPARSE_V4)
        .build()

      val chunkingOptions = HierarchicalChunkerOptions.builder()
        .useMarkdownTables(useMarkdownTables)
        .build()

      val request = HierarchicalChunkDocumentRequest.builder()
        .chunkingOptions(chunkingOptions)
        .options(convertOptions)
        .build()

      val response = client.chunkFilesWithHierarchicalChunker(request, path)
      parseChunkResponse(response)
    }
  }

  /**
    * Experimental: Not used for now vs chunkWithHierarchical
    * Chunk document using Docling HybridChunker with custom options.
    * Hybrid chunker combines hierarchical chunking with tokenization-aware refinements.
    *
    * @param path       Path to the document file
    * @param maxTokens  Maximum number of tokens per chunk
    * @param mergePeers Whether to merge consecutive chunks with same headings/captions
    * @return Try containing list of chunks or failure
    */
  def chunkWithHybrid(path: Path, maxTokens: Int, mergePeers: Boolean = true): Try[List[DoclingChunk]] = {
    val fileName = path.getFileName.toString
    logger.info(s"Chunking document via Docling HybridChunker: $fileName (maxTokens=$maxTokens)")

    Try {
      import ai.docling.serve.api.chunk.request.HybridChunkDocumentRequest
      import ai.docling.serve.api.chunk.request.options.HybridChunkerOptions
      import ai.docling.serve.api.convert.request.options.{ConvertDocumentOptions, PdfBackend}

      val convertOptions = ConvertDocumentOptions.builder()
        .doOcr(false)
        .pdfBackend(PdfBackend.DLPARSE_V4)
        .build()

      val chunkingOptions = HybridChunkerOptions.builder()
        .maxTokens(maxTokens)
        .mergePeers(mergePeers)
        .build()

      val request = HybridChunkDocumentRequest.builder()
        .chunkingOptions(chunkingOptions)
        .options(convertOptions)
        .build()

      val response = client.chunkFilesWithHybridChunker(request, path)
      parseChunkResponse(response)
    }
  }

  /**
    * Chunk document using Docling:
    * - HybridChunker: Pass maxTokens to limit chunk size (200-300 is a good range)
    * - HierarchicalChunker
    *
    * @param path Path to the document file
    * @return Try containing list of [[TextSegments]] ready for embedding or failure
    */
  def chunkToTextSegments(path: Path): Try[List[TextSegment]] = {
    chunkWithHybrid(path, 300).map(_.map(_.toTextSegment))
    //chunkWithHierarchical(path, false).map(_.map(_.toTextSegment))
  }

  private def parseChunkResponse(response: ai.docling.serve.api.chunk.response.ChunkDocumentResponse): List[DoclingChunk] = {
    val chunks = response.getChunks.asScala.toList
    logger.info(s"Docling returned: ${chunks.size} chunks")

    chunks.map { chunk =>
      DoclingChunk(
        text = chunk.getText,
        headings = Option(chunk.getHeadings).map(_.asScala.toList).getOrElse(Nil),
        captions = Option(chunk.getCaptions).map(_.asScala.toList).getOrElse(Nil),
        pageNumbers = Option(chunk.getPageNumbers).map(_.asScala.toList.map(_.intValue())).getOrElse(Nil),
        filename = chunk.getFilename,
        chunkIndex = chunk.getChunkIndex
      )
    }
  }
}

package rag.core

/**
  * A source chunk represents a single retrieved text segment with its associated metadata.
  * Used to provide source attribution for RAG responses.
  *
  * @param fileName     Name of the PDF file this chunk came from
  * @param pageCount    Total number of pages in the source document
  * @param chunkIndex   Index of this chunk within the document (0-based)
  * @param chunkText    The actual text content of the chunk
  * @param score        Relevance score from embedding search (0.0 to 1.0)
  * @param title        Document title if available
  * @param author       Document author if available
  * @param creationDate Document creation date if available
  * @param subject      Document subject if available
  * @param pageNumbers  Page numbers this chunk spans (from Docling chunking); Nil for Tika-parsed documents
  * @param reranked     Whether this chunk survived Cohere reranking
  */
case class SourceChunk(
                        fileName: String,
                        pageCount: Int,
                        chunkIndex: Int,
                        chunkText: String,
                        score: Double,
                        title: Option[String] = None,
                        author: Option[String] = None,
                        creationDate: Option[String] = None,
                        subject: Option[String] = None,
                        pageNumbers: List[Int] = Nil,
                        reranked: Boolean = false
                      )

/**
  * Rich chat response containing both the LLM answer and source attribution.
  * Provides complete context about how the answer was generated and which documents
  * were consulted.
  *
  * @param answer               The final answer from the LLM (empty when using prepareQuery)
  * @param sources              List of chunks that were retrieved and used for RAG
  * @param totalChunksRetrieved Total number of chunks retrieved from embedding store
  *                             (can be higher than sources.length if some were filtered)
  * @param rerankingStats       Optional stats showing how many chunks survived reranking
  * @param preparedQuery        Optional assembled prompt ready to send to an LLM (system message + context + query)
  */
case class RichChatResponse(
                             answer: String,
                             sources: List[SourceChunk],
                             totalChunksRetrieved: Int,
                             rerankingStats: Option[RerankingStats] = None,
                             preparedQuery: Option[String] = None
                           ) {
  /**
    * Returns a human-readable summary of the reranking effect
    */
  def rerankingSummary: String = rerankingStats match {
    case Some(stats) =>
      s"${stats.inputCount} chunks retrieved → ${stats.outputCount} relevant (${stats.chunksFiltered} filtered by Cohere reranking)"
    case None =>
      s"$totalChunksRetrieved chunks retrieved (reranking disabled)"
  }
}

/**
  * A lightweight projection of [[SourceChunk]] for REST API responses
  *
  * @param fileName    Name of the source PDF
  * @param author      Document author if available
  * @param score       Relevance score (0-1)
  * @param preview     First N characters of the chunk for preview
  * @param pageNumbers Page numbers this chunk spans (from Docling chunking); Nil for Tika-parsed documents
  * @param reranked    Whether this chunk survived Cohere reranking
  */
case class SourceSummary(
                          fileName: String,
                          author: Option[String],
                          score: Double,
                          preview: String,
                          pageNumbers: List[Int] = Nil,
                          reranked: Boolean = false
                        )

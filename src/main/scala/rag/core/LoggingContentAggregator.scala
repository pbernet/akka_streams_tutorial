package rag.core

import dev.langchain4j.rag.content.Content
import dev.langchain4j.rag.content.aggregator.ContentAggregator
import dev.langchain4j.rag.query.Query
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.*
import scala.util.Try

/**
  * Captures reranking statistics (input count, output count) for reporting.
  *
  * @param inputCount  Number of chunks before reranking
  * @param outputCount Number of chunks after reranking (those above minScore)
  */
case class RerankingStats(inputCount: Int, outputCount: Int) {
  def chunksFiltered: Int = inputCount - outputCount
  def retentionRate: Double = if (inputCount > 0) outputCount.toDouble / inputCount else 0.0
}

/**
  * A [[ContentAggregator]] wrapper that also logs and captures reranking statistics
  * and the full pre/post-rerank Content objects for source attribution.
  *
  * Captures input contents (pre-rerank) to derive SourceChunks, and output
  * contents (post-rerank) to mark which chunks survived Cohere reranking.
  *
  * @param delegate  The underlying ContentAggregator to wrap
  * @param reranking Whether the delegate performs actual reranking (e.g. Cohere).
  *                  When false (= passthrough mode), chunks are not marked as reranked.
  */
class LoggingContentAggregator(delegate: ContentAggregator, reranking: Boolean) extends ContentAggregator {
  private val logger = LoggerFactory.getLogger(classOf[LoggingContentAggregator])

  private val lastStats = new AtomicReference[Option[RerankingStats]](None)
  private val lastInputContents = new AtomicReference[List[Content]](List.empty)
  private val lastOutputContents = new AtomicReference[List[Content]](List.empty)
  
  override def aggregate(queryToContents: java.util.Map[Query, java.util.Collection[java.util.List[Content]]]): java.util.List[Content] = {
    val inputs = queryToContents.values().asScala
      .flatMap(_.asScala)
      .flatMap(_.asScala)
      .toList
    lastInputContents.set(inputs)
    
    val result = delegate.aggregate(queryToContents)
    val outputs = result.asScala.toList
    lastOutputContents.set(outputs)

    val stats = RerankingStats(inputs.size, outputs.size)
    lastStats.set(Some(stats))

    if (reranking) {
      logger.info(s"Reranking: ${inputs.size} chunks → ${outputs.size} chunks retained (${stats.chunksFiltered} filtered, ${f"${stats.retentionRate * 100}%.0f"}% retention)")
    } else {
      logger.info(s"Content aggregation: ${outputs.size} chunks captured for source attribution")
    }
    
    result
  }

  def getLastStats: Option[RerankingStats] = if (reranking) lastStats.get() else None

  /**
    * Derive SourceChunks from the captured pre-rerank Content objects,
    * marking each chunk with reranked=true if it survived reranking.
    */
  def getSourceChunks: List[SourceChunk] = {
    val inputs = lastInputContents.get()
    val outputTexts = lastOutputContents.get().map(_.textSegment().text()).toSet

    inputs.zipWithIndex.map { case (content, idx) =>
      val segment = content.textSegment()
      val metadata = segment.metadata()
      val text = segment.text()

      val fileName = Option(metadata.getString("fileName")).getOrElse("unknown.pdf")
      val pageCount = Try(Option(metadata.getString("pageCount")).getOrElse("0").toInt).getOrElse(0)
      val title = Option(metadata.getString("title"))
      val author = Option(metadata.getString("author"))
      val subject = Option(metadata.getString("subject"))
      val creationDate = Option(metadata.getString("creationDate"))
      val pageNumbers = Option(metadata.getString("pageNumbers"))
        .filter(_.nonEmpty)
        .map(_.split(",").flatMap(s => Try(s.trim.toInt).toOption).toList)
        .getOrElse(Nil)

      SourceChunk(
        fileName = fileName,
        pageCount = pageCount,
        chunkIndex = idx,
        chunkText = text,
        similarity = Try(Option(metadata.getString("similarity")).getOrElse("0.0").toDouble).getOrElse(0.0),
        title = title,
        author = author,
        creationDate = creationDate,
        subject = subject,
        pageNumbers = pageNumbers,
        reranked = reranking && outputTexts.contains(text)
      )
    }
  }

  def clearStats(): Unit = {
    lastStats.set(None)
    lastInputContents.set(List.empty)
    lastOutputContents.set(List.empty)
  }
}

object LoggingContentAggregator {
  def apply(delegate: ContentAggregator, reranking: Boolean = false): LoggingContentAggregator =
    new LoggingContentAggregator(delegate, reranking)
}

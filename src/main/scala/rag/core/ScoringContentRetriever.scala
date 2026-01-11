package rag.core

import dev.langchain4j.data.segment.TextSegment
import dev.langchain4j.model.embedding.EmbeddingModel
import dev.langchain4j.rag.content.Content
import dev.langchain4j.rag.content.retriever.ContentRetriever
import dev.langchain4j.rag.query.Query
import dev.langchain4j.store.embedding.{EmbeddingSearchRequest, EmbeddingStore}
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters.*

/**
  * ContentRetriever that injects each match's embedding similarity score into
  * the TextSegment metadata under the key "similarity", so it survives through
  * ContentAggregator pipelines (including Cohere reranking) for source attribution.
  */
class ScoringContentRetriever(
                               embeddingStore: EmbeddingStore[TextSegment],
                               embeddingModel: EmbeddingModel,
                               maxResults: Int = 10,
                               minScore: Double = 0.6
                             ) extends ContentRetriever {

  private val logger = LoggerFactory.getLogger(classOf[ScoringContentRetriever])

  override def retrieve(query: Query): java.util.List[Content] = {
    val embedding = embeddingModel.embed(query.text()).content()
    val request = EmbeddingSearchRequest.builder()
      .queryEmbedding(embedding)
      .maxResults(maxResults)
      .minScore(minScore)
      .build()

    val matches = embeddingStore.search(request).matches().asScala.toList
    logger.debug(s"Retrieved: ${matches.size} matches (maxResults=$maxResults, minScore=$minScore)")

    matches.map { match_ =>
      val segment = match_.embedded()
      segment.metadata().put("similarity", match_.score().toString)
      Content.from(segment)
    }.asJava
  }
}

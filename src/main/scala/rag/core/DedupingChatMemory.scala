package rag.core

import dev.langchain4j.data.message.ChatMessage
import dev.langchain4j.memory.ChatMemory
import org.slf4j.{Logger, LoggerFactory}

import java.security.MessageDigest
import scala.jdk.CollectionConverters.*

/**
  * Wraps [[ChatMemory]] to deduplicate messages based on content hash.
  */
class DedupingChatMemory(delegate: ChatMemory) extends ChatMemory {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private var messageHashes: Set[String] = Set.empty

  override def add(message: ChatMessage): Unit = {
    val hash = hashMessage(message)
    if (!messageHashes.contains(hash)) {
      messageHashes = messageHashes + hash
      delegate.add(message)
      logger.debug(s"Added message: ${message.getClass.getSimpleName}")
    } else {
      logger.debug(s"Skipped duplicate message: ${message.getClass.getSimpleName}")
    }
  }

  override def messages(): java.util.List[ChatMessage] = {
    val allMessages = delegate.messages()
    messageHashes = allMessages.asScala.map(hashMessage).toSet
    allMessages
  }

  override def clear(): Unit = {
    delegate.clear()
    messageHashes = Set.empty
  }

  override def id(): Any = delegate.id()

  private def hashMessage(message: ChatMessage): String = {
    val md = MessageDigest.getInstance("SHA-256")
    val hashBytes = md.digest(message.toString.getBytes("UTF-8"))
    hashBytes.map("%02x".format(_)).mkString
  }
}

package rag.core

import org.apache.pekko.actor.ActorSystem
import org.slf4j.{Logger, LoggerFactory}

object RagEngineProvider {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  @volatile private var engine: RagEngine = _

  def get()(implicit system: ActorSystem): RagEngine = {
    if (engine == null) {
      synchronized {
        if (engine == null) {
          logger.info("Creating and initializing shared RagEngine instance...")
          val e = RagEngine(dropTableFirst = true)
          e.initialize()
          engine = e
          logger.info("RagEngine singleton initialized successfully")
        }
      }
    }
    engine
  }
}

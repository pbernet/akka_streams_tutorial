package rag.core

import java.nio.file.Path

object ParserFactory {
  private val parsers: List[DocumentParser] = List(
    TikaDocumentParser,
    PlainTextParser
  )

  def getParserFor(path: Path): Option[DocumentParser] =
    parsers.find(_.canParse(path))
}

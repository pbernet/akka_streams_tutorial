package rag.core

import org.apache.pdfbox.Loader
import org.apache.pdfbox.io.RandomAccessReadBufferedFile
import org.apache.tika.Tika
import org.apache.tika.metadata.{Metadata, TikaCoreProperties}
import org.slf4j.LoggerFactory

import java.io.FileInputStream
import java.nio.file.{Files, Path}
import java.text.SimpleDateFormat
import scala.util.{Failure, Success, Try, Using}

sealed trait DocumentParser {
  def canParse(path: Path): Boolean
  def parse(path: Path): Try[(String, DocumentMetadata)]
}

case object TikaDocumentParser extends DocumentParser {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val supportedExtensions = Set(".pdf", ".docx", ".doc")
  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

  override def canParse(path: Path): Boolean = {
    val fileName = path.toString.toLowerCase
    supportedExtensions.exists(fileName.endsWith)
  }

  override def parse(path: Path): Try[(String, DocumentMetadata)] = {
    val fileName = path.getFileName.toString
    logger.info(s"Parsing document: $fileName")

    Try {
      val tika = new Tika()
      val text = tika.parseToString(path.toFile)

      val metadata = if (fileName.toLowerCase.endsWith(".pdf")) {
        extractPdfMetadata(path, fileName)
      } else {
        extractTikaMetadata(path, fileName)
      }

      logger.info(s"Successfully parsed: $fileName, extracted: ${text.length} characters")
      (text, metadata)
    } match {
      case Success(result) => Success(result)
      case Failure(ex) =>
        logger.warn(s"Failed to parse: $fileName: ${ex.getMessage}")
        Failure(ex)
    }
  }

  private def extractPdfMetadata(path: Path, fileName: String): DocumentMetadata = {
    Try {
      val file = new RandomAccessReadBufferedFile(path.toFile)
      val document = Loader.loadPDF(file)
      try {
        val pageCount = document.getNumberOfPages
        val docInfo = document.getDocumentInformation
        val metadata = DocumentMetadata(
          fileName = fileName,
          pageCount = Some(pageCount),
          title = Option(docInfo.getTitle),
          author = Option(docInfo.getAuthor),
          subject = Option(docInfo.getSubject),
          creationDate = Option(docInfo.getCreationDate).map(cal => dateFormat.format(cal.getTime)),
          producerApp = Option(docInfo.getProducer)
        )
        logger.info(s"Extracted PDF metadata: $fileName - Pages: $pageCount, Title: ${metadata.title}, Author: ${metadata.author}")
        metadata
      } finally {
        document.close()
      }
    } match {
      case Success(metadata) => metadata
      case Failure(ex) =>
        logger.warn(s"Failed to extract PDF metadata from $fileName: ${ex.getMessage}")
        DocumentMetadata(fileName = fileName)
    }
  }

  private def extractTikaMetadata(path: Path, fileName: String): DocumentMetadata = {
    Try {
      val tika = new Tika()
      val tikaMetadata = new Metadata()

      Using(new FileInputStream(path.toFile)) { inputStream =>
        tika.parse(inputStream, tikaMetadata)

        DocumentMetadata(
          fileName = fileName,
          title = Option(tikaMetadata.get(TikaCoreProperties.TITLE)),
          author = Option(tikaMetadata.get(TikaCoreProperties.CREATOR)),
          subject = Option(tikaMetadata.get(TikaCoreProperties.SUBJECT)),
          creationDate = Option(tikaMetadata.get(TikaCoreProperties.CREATED)).map(_.toString),
          producerApp = Option(tikaMetadata.get("Application-Name"))
            .orElse(Option(tikaMetadata.get("producer")))
        )
      }.get
    } match {
      case Success(metadata) =>
        logger.info(s"Extracted Tika metadata: $fileName - Title: ${metadata.title}, Author: ${metadata.author}")
        metadata
      case Failure(ex) =>
        logger.warn(s"Failed to extract Tika metadata from $fileName: ${ex.getMessage}")
        DocumentMetadata(fileName = fileName)
    }
  }
}

case object PlainTextParser extends DocumentParser {
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def canParse(path: Path): Boolean =
    path.toString.toLowerCase.endsWith(".txt")

  override def parse(path: Path): Try[(String, DocumentMetadata)] = {
    val fileName = path.getFileName.toString
    logger.info(s"Parsing text file: $fileName")

    Try {
      val text = Files.readString(path)
      val metadata = DocumentMetadata(fileName = fileName)
      logger.info(s"Successfully parsed $fileName, extracted ${text.length} characters")
      (text, metadata)
    } match {
      case Success(result) => Success(result)
      case Failure(ex) =>
        logger.warn(s"Failed to parse $fileName: ${ex.getMessage}")
        Failure(ex)
    }
  }
}

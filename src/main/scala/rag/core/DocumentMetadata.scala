package rag.core

case class DocumentMetadata(
                             fileName: String,
                             title: Option[String] = None,
                             author: Option[String] = None,
                             subject: Option[String] = None,
                             creationDate: Option[String] = None,
                             producerApp: Option[String] = None,
                             pageCount: Option[Int] = None,
                             wordCount: Option[Int] = None
                           )

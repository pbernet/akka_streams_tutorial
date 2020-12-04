package example.myapp.helloworld

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future

class GreeterServiceImpl(implicit mat: Materializer) extends GreeterService {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  import mat.executionContext

  override def sayHello(in: HelloRequest): Future[HelloReply] = {
    //Metadata eg client id for now transported as part of payload ${in.clientId}
    logger.info(s"Server received from client: xx with name: ${in.name}")
    //Extend signature
    //Future.successful(HelloReply(s"Hello, ${in.name}", Some(Timestamp.apply(123456, 123))))
    Future.successful(HelloReply(s"Hello, ${in.name}"))
  }

  override def itKeepsTalking(in: Source[HelloRequest, NotUsed]): Future[HelloReply] = {
    logger.info(s"sayHello to in stream...")
    in.runWith(Sink.seq).map(elements => HelloReply(s"Hello, ${elements.map(_.name).mkString(", ")}"))
  }

  override def itKeepsReplying(in: HelloRequest): Source[HelloReply, NotUsed] = {
    logger.info(s"sayHello to ${in.name} with stream of chars...")
    Source(s"Hello, ${in.name}".toList).map(character => HelloReply(character.toString))
  }

  override def streamHellos(in: Source[HelloRequest, NotUsed]): Source[HelloReply, NotUsed] = {
    logger.info(s"sayHello to stream...")
    in.map(request => HelloReply(s"Hello, ${request.name}"))
  }
}

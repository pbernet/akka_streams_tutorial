package akka.grpc.echo

import java.time.Instant

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.google.protobuf.timestamp.Timestamp
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future



class GreeterServiceImpl(implicit mat: Materializer) extends GreeterService {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  import mat.executionContext

  //Metadata attributes for now transported as part of payload
  override def sayHello(in: HelloRequest): Future[HelloReply] = {
    logger.info(s"Server: received msg from client: ${in.clientId} with name: ${in.name}")
    Future.successful(HelloReply(s"Hello, ${in.name}", Some(Timestamp.apply(Instant.now().getEpochSecond, 0))))
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

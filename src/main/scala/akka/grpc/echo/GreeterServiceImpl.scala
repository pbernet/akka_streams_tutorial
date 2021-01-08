package akka.grpc.echo

import java.time.Instant

import akka.NotUsed
import akka.grpc.echo.gen._
import akka.stream.Materializer
import akka.stream.scaladsl.{BroadcastHub, Keep, MergeHub, Sink, Source}
import com.google.protobuf.timestamp.Timestamp
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt


/**
  * For simplicity reasons all the grpc server sources are kept here
  *
  * @param mat
  */
class GreeterServiceImpl(implicit mat: Materializer) extends GreeterService {
  import mat.executionContext
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val replyFun = {request: HelloRequest => HelloReply(s"Hello, ${request.name}")}
  val (inboundHub: Sink[HelloRequest, NotUsed], outboundHub: Source[HelloReply, NotUsed]) =
    MergeHub.source[HelloRequest]
      .wireTap(each => logger.debug(s"Received streamHellos request from client: ${each.clientId}"))
      .map(replyFun)
      .toMat(BroadcastHub.sink[HelloReply])(Keep.both)
      .run()

  // Metadata attributes for now transported as part of payload
  override def sayHello(in: HelloRequest): Future[HelloReply] = {
    logger.info(s"Received from client: ${in.clientId} single msg: ${in.name}")
    Future.successful(HelloReply(s"ACK, ${in.name}", Some(Timestamp.apply(Instant.now().getEpochSecond, 0))))
  }

  // Receive a batch/stream of elements and ACK at the end
  override def itKeepsTalking(in: Source[HelloRequest, NotUsed]): Future[HelloReply] = {
    in
      .wireTap(each => logger.info(s"Received from client: ${each.clientId} streamed msg: ${each.name}") )
      .runWith(Sink.seq).map(elements => HelloReply(s"ACK, ${elements.map(_.name).mkString(", ")}"))
  }

  // Example of an application level heartbeat: server -> client
  // vs dedicated Health service, according to spec:
  // https://github.com/grpc/grpc/blob/master/doc/health-checking.md
  // https://github.com/akka/akka-grpc/issues/700
  override def itKeepsReplying(in: HelloRequest): Source[HelloReply, NotUsed] = {
    logger.info(s"Starting heartbeat...")

    Source
      .tick(initialDelay = 0.seconds, interval = 1.seconds, NotUsed)
      .map(each => HelloReply(each.toString, Some(Timestamp.apply(Instant.now().getEpochSecond, 0))))
      .mapMaterializedValue(_ => NotUsed)
  }

  override def streamHellos(in: Source[HelloRequest, NotUsed]): Source[HelloReply, NotUsed] = {

    //Reply to asking client only
    //in.map(replyFun)

    // Reply to all clients, like a chat room
    in.runWith(inboundHub)
    outboundHub
  }
}

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



class GreeterServiceImpl(implicit mat: Materializer) extends GreeterService {
  import mat.executionContext
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val replyFun = {request: HelloRequest => HelloReply(s"Hello, ${request.name}")}
  val (inboundHub: Sink[HelloRequest, NotUsed], outboundHub: Source[HelloReply, NotUsed]) =
    MergeHub.source[HelloRequest]
      .wireTap(each => logger.info(s"Server received streamHellos request from client: ${each.clientId}"))
      .map(replyFun)
      .toMat(BroadcastHub.sink[HelloReply])(Keep.both)
      .run()

  // Metadata attributes for now transported as part of payload
  override def sayHello(in: HelloRequest): Future[HelloReply] = {
    logger.info(s"Server received single msg from client: ${in.clientId} with name: ${in.name}")
    Future.successful(HelloReply(s"Hello, ${in.name}", Some(Timestamp.apply(Instant.now().getEpochSecond, 0))))
  }

  // Utilize to send a batch of data and ack together
  override def itKeepsTalking(in: Source[HelloRequest, NotUsed]): Future[HelloReply] = {
    in
      .wireTap(each => logger.info(s"Server received stream of msgs from client: ${each.clientId} with name: ${each.name}") )
      .runWith(Sink.seq).map(elements => HelloReply(s"Hello, ${elements.map(_.name).mkString(", ")}"))
  }

  // Utilize to have heartbeat functionality on the application level
  // TODO vs https://github.com/grpc/grpc/blob/master/doc/health-checking.md
  override def itKeepsReplying(in: HelloRequest): Source[HelloReply, NotUsed] = {
    logger.info(s"Starting heartbeat...")

    Source
      .tick(initialDelay = 0.seconds, interval = 1.seconds, NotUsed)
      .map(each => HelloReply(each.toString, Some(Timestamp.apply(Instant.now().getEpochSecond, 0))))
      .mapMaterializedValue(each => NotUsed)
  }

  override def streamHellos(in: Source[HelloRequest, NotUsed]): Source[HelloReply, NotUsed] = {

    //Reply to asking client only
    //in.map(replyFun)

    // Reply to all clients, like a chat room
    in.runWith(inboundHub)
    outboundHub
  }
}

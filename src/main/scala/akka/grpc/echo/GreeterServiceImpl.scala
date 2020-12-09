package akka.grpc.echo

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.{BroadcastHub, Keep, MergeHub, Sink, Source}
import akka.stream.{Materializer, ThrottleMode}
import com.google.protobuf.timestamp.Timestamp
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt



class GreeterServiceImpl(implicit mat: Materializer) extends GreeterService {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val replyFun = {request: HelloRequest => HelloReply(s"Hello, ${request.name}")}
  val (inboundHub: Sink[HelloRequest, NotUsed], outboundHub: Source[HelloReply, NotUsed]) =
    MergeHub.source[HelloRequest]
      .map(replyFun)
      .toMat(BroadcastHub.sink[HelloReply])(Keep.both)
      .run()

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
    logger.info(s"Starting heartbeat...")
    Source(1 to 100)
      .throttle(1, 1.second, 10, ThrottleMode.shaping)
      .map(each => HelloReply(each.toString))
  }


  //TODO Add param how to reply (single vs chat) as part of request?
  override def streamHellos(in: Source[HelloRequest, NotUsed]): Source[HelloReply, NotUsed] = {
    logger.info(s"sayHello to stream...")
    //Reply to asking client
    in.map(replyFun)

    // Reply to all clients, "chat server"
    // The stream of incoming HelloRequest messages are sent out as corresponding HelloReply.
    // From all clients to all clients, like a chat room.
    in.runWith(inboundHub)
    outboundHub
  }
}

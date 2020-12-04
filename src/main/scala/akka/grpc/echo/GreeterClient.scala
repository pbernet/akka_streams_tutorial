package akka.grpc.echo

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

object GreeterClient extends App {
  implicit val system = ActorSystem("GreeterClient")
  implicit val executionContext = system.dispatcher

  // Configure the client by code:
  val clientSettings = GrpcClientSettings
    .connectToServiceAt("127.0.0.1", 8080)
    //.withDeadline(1.second) //TODO Check impact and need
    .withTls(false)


  // Or via application.conf:
  // And via service discovery https://doc.akka.io/docs/akka-grpc/current/client/configuration.html#using-akka-discovery-for-endpoint-discovery
  // val clientSettings = GrpcClientSettings.fromConfig(GreeterService.name)

  // Create a client-side stub for the service
  val client: GreeterService = GreeterServiceClient(clientSettings)

  // Run examples for each of the exposed service methods.
  runSingleRequestReplyExample()
  runStreamingRequestExample()
  runStreamingReplyExample()
  runStreamingRequestReplyExample()

  system.scheduler.schedule(1.second, 1.second) {
    runSingleRequestReplyExample()
  }

  def runSingleRequestReplyExample(): Unit = {
    system.log.info("Performing request")
    val reply = client.sayHello(HelloRequest("Alice"))
    reply.onComplete {
      case Success(msg) =>
        println(s"got single reply: $msg")
      case Failure(e) =>
        println(s"Error sayHello: $e")
    }
  }

  def runStreamingRequestExample(): Unit = {
    val requests = List("Alice", "Bob", "Peter").map(HelloRequest(_))
    val reply = client.itKeepsTalking(Source(requests))
    reply.onComplete {
      case Success(msg) =>
        println(s"got single reply for streaming requests: $msg")
      case Failure(e) =>
        println(s"Error streamingRequest: $e")
    }
  }

  def runStreamingReplyExample(): Unit = {
    val responseStream = client.itKeepsReplying(HelloRequest("Alice"))
    val done: Future[Done] =
      responseStream.runForeach(reply => println(s"got streaming reply: ${reply.message}"))

    done.onComplete {
      case Success(_) =>
        println("streamingReply done")
      case Failure(e) =>
        println(s"Error streamingReply: $e")
    }
  }

  def runStreamingRequestReplyExample(): Unit = {
    val requestStream: Source[HelloRequest, NotUsed] =
      Source
        .tick(100.millis, 1.second, "tick")
        .zipWithIndex
        .map { case (_, i) => i }
        .map(i => HelloRequest(s"Alice-$i"))
        .take(10)
        .mapMaterializedValue(_ => NotUsed)

    val responseStream: Source[HelloReply, NotUsed] = client.streamHellos(requestStream)
    val done: Future[Done] =
      responseStream.runForeach(reply => println(s"got streaming reply: ${reply.message}"))

    done.onComplete {
      case Success(_) =>
        println("streamingRequestReply done")
      case Failure(e) =>
        println(s"Error streamingRequestReply: $e")
    }
  }
}

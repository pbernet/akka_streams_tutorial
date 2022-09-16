package akka.grpc.echo

import akka.actor.ActorSystem
import akka.grpc.echo.gen._
import akka.http.scaladsl.Http
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

/**
  * gRPC server to serve [[GreeterClient]]
  * Protobuf resource file: src/main/protobuf/grpcecho.proto
  *
  * As an additional client you may run grpcui,
  * a command-line tool that lets you interact with gRPC servers via a browser
  *
  * Run on docker with cmd:
  * docker run -it --rm -p 0.0.0.0:8090:8090 fullstorydev/grpcui:latest -plaintext -port 8090 -vv host.docker.internal:8081
  *
  * and then access grpcui via:
  * http://localhost:8090
  *
  * Doc:
  * https://github.com/fullstorydev/grpcui
  */
object GreeterServer extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("GreeterServer")
  implicit val ec = system.dispatcher

  val (host, port) = ("127.0.0.1", 8081)

  // To serve multiple services, see:
  // https://doc.akka.io/docs/akka-grpc/current/server/walkthrough.html#serving-multiple-services
  val service = GreeterServiceHandler.withServerReflection(new GreeterServiceImpl())

  val bound: Future[Http.ServerBinding] = Http(system)
    .newServerAt(host, port)
    .bind(service)
    .map(_.addToCoordinatedShutdown(hardTerminationDeadline = 10.seconds))

  bound.onComplete {
    case Success(binding) =>
      val address = binding.localAddress
      logger.info(s"gRPC server bound to: ${address.getHostString}:${address.getPort}")
    case Failure(ex) =>
      logger.info("Failed to bind gRPC endpoint, terminating system", ex)
      system.terminate()
  }
}
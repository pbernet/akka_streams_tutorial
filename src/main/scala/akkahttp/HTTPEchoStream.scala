package akkahttp

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.common.{EntityStreamingSupport, JsonEntityStreamingSupport}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{complete, get, logRequestResult, path, _}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import spray.json.DefaultJsonProtocol

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * 1st version
  * Focus on download stream
  *
  * Doc streaming implications:
  * https://doc.akka.io/docs/akka-http/current/implications-of-streaming-http-entity.html#implications-of-the-streaming-nature-of-request-response-entities
  *
  * Doc JSON streaming support:
  * https://doc.akka.io/docs/akka-http/current/routing-dsl/source-streaming-support.html
  *
  * Doc Consuming streaming APIs
  * https://doc.akka.io/docs/akka-http/current/common/json-support.html
  */
object HTTPEchoStream extends App with DefaultJsonProtocol with SprayJsonSupport {
  implicit val system = ActorSystem("HTTPEchoStream")
  implicit val executionContext = system.dispatcher
  implicit val materializerServer = ActorMaterializer()

  //JSON Protocol and streaming support
  final case class ExamplePerson(name: String)
  implicit def examplePersonFormat = jsonFormat1(ExamplePerson.apply)
  implicit val jsonStreamingSupport: JsonEntityStreamingSupport =
    EntityStreamingSupport.json()

  val (address, port) = ("127.0.0.1", 8080)
  server(address, port)
  (1 to 1).par.foreach(each => clientDownload(each, address, port))

  def clientDownload(each: Int, address: String, port: Int): Unit = {

    val requests: Source[HttpRequest, NotUsed] = Source
      .fromIterator(() =>
        Range(0, 10).map(i => HttpRequest(uri = Uri(s"http://$address:$port/download/$i"))).iterator
      )

    // Run and completely consume a single akka http request
    def runRequestDownload(req: HttpRequest) =
      Http()
        .singleRequest(req)
        .flatMap { response =>
          val unmarshalled: Future[Source[ExamplePerson, NotUsed]] =
            Unmarshal(response).to[Source[ExamplePerson, NotUsed]]

          // flatten the Future[Source[]] into a Source[]:
          val source: Source[ExamplePerson, Future[NotUsed]] =
            Source.fromFutureSource(unmarshalled)

          source.runForeach(println(_))
        }

    // Run each akka http flow to completion, then continue processing. You'll want to tune the `parallelism`
    // parameter to mapAsync -- higher values will create more cpu and memory load which may or may not positively
    // impact performance.
    requests
      .mapAsync(1)(runRequestDownload)
      //.via(processorFlow)
      .runWith(Sink.ignore)

  }

  def server(address: String, port: Int): Unit = {

    val stream: Stream[ExamplePerson] = Stream.continually(ExamplePerson("test")).take(5)

    def routes: Route = logRequestResult("httpecho") {
      path("download" / Segment) { id: String =>

        get {
          println(s"Server received download request for: $id ")
           withoutSizeLimit {
            extractRequest { r: HttpRequest =>
              val finishedWriting = r.discardEntityBytes().future
              // we only want to respond once the incoming data has been handled:
              onComplete(finishedWriting) { done =>
                //Ongoing request [GET /download/0 Empty] was dropped because pool is shutting down
                //https://github.com/akka/akka-http/pull/2630
                complete(Source(stream))
              }
            }
          }
        }
      }

      //      ~
      //        path("xx") {
      //          get {
      //              println(s"Server received xxx request for:")
      //              complete(StatusCodes.OK)
      //            }
      //          }
    }

    val bindingFuture = Http().bindAndHandle(routes, address, port)
    bindingFuture.onComplete {
      case Success(b) =>
        println("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        println(s"Server could not bind to $address:$port. Exception message: ${e.getMessage}")
        system.terminate()
    }
  }
}
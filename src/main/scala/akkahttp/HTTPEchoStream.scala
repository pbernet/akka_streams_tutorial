package akkahttp

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.common.{EntityStreamingSupport, JsonEntityStreamingSupport}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{complete, get, logRequestResult, path, _}
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString
import spray.json.DefaultJsonProtocol

import scala.concurrent.Future
import scala.util.{Failure, Success}

trait JsonProtocol3 extends DefaultJsonProtocol with SprayJsonSupport {

  final case class ExamplePerson(name: String)

  implicit def examplePersonFormat = jsonFormat1(ExamplePerson.apply)
}

/**
  * 1st version
  * Focus on download stream
  *
  * Doc streaming implications:
  * https://doc.akka.io/docs/akka-http/current/implications-of-streaming-http-entity.html#implications-of-the-streaming-nature-of-request-response-entities
  *
  * Doc JSON streaming support:
  * https://doc.akka.io/docs/akka-http/current/routing-dsl/source-streaming-support.html
  */
object HTTPEchoStream extends App with JsonProtocol3 {
  implicit val system = ActorSystem("HTTPEchoStream")
  implicit val executionContext = system.dispatcher
  implicit val materializerServer = ActorMaterializer()

  val (address, port) = ("127.0.0.1", 8080)
  server(address, port)
  (1 to 1).par.foreach(each => clientDownload(each, address, port))


  def clientDownload(each: Int, address: String, port: Int): Unit = {

    def parse(line: ByteString): Option[ExamplePerson] =
    line.utf8String.split(" ").headOption.map(ExamplePerson)

  val requests: Source[HttpRequest, NotUsed] = Source
    .fromIterator(() =>
      Range(0, 10).map(i => HttpRequest(uri = Uri(s"http://$address:$port/download/$i"))).iterator
    )

  val processorFlow: Flow[Option[ExamplePerson], Int, NotUsed] =
    Flow[Option[ExamplePerson]].map(_.map(_.name.length).getOrElse(0))

  // Run and completely consume a single akka http request
  def runRequestDownload(req: HttpRequest): Future[Option[ExamplePerson]] =
    Http()
      .singleRequest(req)
      .flatMap { response =>
        response.entity.dataBytes
          .runReduce(_ ++ _)
          .map(parse)
      }

    val printSink = Sink.foreach[Int] { each => println(each)}

  // Run each akka http flow to completion, then continue processing. You'll want to tune the `parallelism`
  // parameter to mapAsync -- higher values will create more cpu and memory load which may or may not positively
  // impact performance.
  requests
    .mapAsync(2)(runRequestDownload)
    .via(processorFlow)
    .runWith(printSink)

  }

  def server(address: String, port: Int): Unit ={

    val stream: Stream[ExamplePerson] = Stream.continually(ExamplePerson("test"))
    implicit val jsonStreamingSupport: JsonEntityStreamingSupport = EntityStreamingSupport.json()

    def routes: Route = logRequestResult("httpecho") {
      path("download" / Segment) { id: String =>

        get {
        println(s"Server received download request for: $id ")

          //Ongoing request [GET /download/0 Empty] was dropped because pool is shutting down
          //https://github.com/akka/akka-http/pull/2630
          complete(Source(stream))

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
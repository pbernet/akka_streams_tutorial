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
import akka.stream.scaladsl.{Flow, Sink, Source}
import spray.json.DefaultJsonProtocol

import scala.util.{Failure, Success}

/**
  * Initiate n HTTP requests from akka-http client and for each consume a bounded stream of elements from server
  * Similar to SSEHeartbeat
  *
  * Doc streaming implications:
  * https://doc.akka.io/docs/akka-http/current/implications-of-streaming-http-entity.html#implications-of-the-streaming-nature-of-request-response-entities
  *
  * Doc JSON streaming support:
  * https://doc.akka.io/docs/akka-http/current/routing-dsl/source-streaming-support.html
  *
  * Doc Consuming JSON streaming APIs
  * https://doc.akka.io/docs/akka-http/current/common/json-support.html
  */
object HTTPDownloadStream extends App with DefaultJsonProtocol with SprayJsonSupport {
  implicit val system = ActorSystem("HTTPDownloadStream")
  implicit val executionContext = system.dispatcher
  implicit val materializerServer = ActorMaterializer()

  //JSON Protocol and streaming support
  final case class ExamplePerson(name: String)

  implicit def examplePersonFormat = jsonFormat1(ExamplePerson.apply)

  implicit val jsonStreamingSupport: JsonEntityStreamingSupport = EntityStreamingSupport.json()

  val (address, port) = ("127.0.0.1", 8080)
  server(address, port)
  (1 to 2).par.foreach(each => client(each, address, port))

  def client(each: Int, address: String, port: Int): Unit = {

    val requests: Source[HttpRequest, NotUsed] = Source
      .fromIterator(() =>
        Range(0, 10).map(i => HttpRequest(uri = Uri(s"http://$address:$port/download/$i"))).iterator
      )


    // Run and completely consume a single akka http request
    def runRequestDownload(req: HttpRequest) =
      Http()
        .singleRequest(req)
        .flatMap { response =>
          val unmarshalled = Unmarshal(response).to[Source[ExamplePerson, NotUsed]]
          val source = Source.fromFutureSource(unmarshalled)
          source.runForeach(i => println(s"Client received: $i"))
        }

    requests
      .mapAsync(2)(runRequestDownload)
      //.via(processorFlow)
      .runWith(Sink.ignore)
  }

  //TODO Try to use printSink and processorFlow, trouble with types...
  val printSink = Sink.foreach[ExamplePerson] { each: ExamplePerson => println(s"Client received: $each") }

  val processorFlow: Flow[ExamplePerson, ExamplePerson, NotUsed] = Flow[ExamplePerson].map {
    each: ExamplePerson => {
      each
    }
  }


  def server(address: String, port: Int): Unit = {

    def routes: Route = logRequestResult("httpecho") {
      path("download" / Segment) { id: String =>
        get {
          println(s"Server received download request for: $id ")
          extractRequest { r: HttpRequest =>
            val finishedWriting = r.discardEntityBytes().future
            // we only want to respond once the incoming data has been handled
            onComplete(finishedWriting) { done =>
              val stream: Stream[ExamplePerson] = Stream.continually(ExamplePerson(s"test$id")).take(5)
              complete(Source(stream))
            }
          }
        }
      }
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
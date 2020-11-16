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
import akka.stream.ThrottleMode
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.typesafe.config.ConfigFactory
import spray.json.DefaultJsonProtocol

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Initiate n singleRequest and in the response consume the stream of elements from the server
  * From client point of view similar to SSEHeartbeat
  *
  * Doc streaming implications:
  * https://doc.akka.io/docs/akka-http/current/implications-of-streaming-http-entity.html#implications-of-the-streaming-nature-of-request-response-entities
  *
  * Doc JSON streaming support:
  * https://doc.akka.io/docs/akka-http/current/routing-dsl/source-streaming-support.html
  *
  * Doc consuming JSON streaming APIs
  * https://doc.akka.io/docs/akka-http/current/common/json-support.html
  */
object HTTPResponseStream extends App with DefaultJsonProtocol with SprayJsonSupport {
  implicit val system = ActorSystem("HTTPResponseStream")
  implicit val executionContext = system.dispatcher

  //JSON Protocol and streaming support
  final case class ExamplePerson(name: String)

  implicit def examplePersonFormat = jsonFormat1(ExamplePerson.apply)

  implicit val jsonStreamingSupport: JsonEntityStreamingSupport = EntityStreamingSupport.json()

  val (address, port) = ("127.0.0.1", 8080)
  server(address, port)
  client(address, port)

  def client(address: String, port: Int): Unit = {
    val requestParallelism = ConfigFactory.load.getInt("akka.http.host-connection-pool.max-connections")

    val requests: Source[HttpRequest, NotUsed] = Source
      .fromIterator(() =>
        Range(0, requestParallelism).map(i => HttpRequest(uri = Uri(s"http://$address:$port/download/$i"))).iterator
      )

    // Run singleRequest and completely consume response elements
    def runRequestDownload(req: HttpRequest) =
      Http()
        .singleRequest(req)
        .flatMap { response =>
          val unmarshalled: Future[Source[ExamplePerson, NotUsed]] = Unmarshal(response).to[Source[ExamplePerson, NotUsed]]
          val source: Source[ExamplePerson, Future[NotUsed]] = Source.futureSource(unmarshalled)
          source.via(processorFlow).runWith(printSink)
        }

    requests
      .mapAsync(requestParallelism)(runRequestDownload)
      .runWith(Sink.ignore)
  }


  val printSink = Sink.foreach[ExamplePerson] { each: ExamplePerson => println(s"Client processed element: $each") }

  val processorFlow: Flow[ExamplePerson, ExamplePerson, NotUsed] = Flow[ExamplePerson].map {
    each: ExamplePerson => {
      //println(s"Process: $each")
      each
    }
  }


  def server(address: String, port: Int): Unit = {

    def routes: Route = logRequestResult("httpecho") {
      path("download" / Segment) { id: String =>
        get {
          println(s"Server received request with id: $id, stream response...")
          extractRequest { r: HttpRequest =>
            val finishedWriting = r.discardEntityBytes().future
            onComplete(finishedWriting) { done =>
              //Limit response by appending eg .take(5)
              val responseStream: LazyList[ExamplePerson] = LazyList.continually(ExamplePerson(s"request:$id"))
              complete(Source(responseStream).throttle(1, 1.second, 1, ThrottleMode.shaping))
            }
          }
        }
      }
    }

    val bindingFuture = Http().newServerAt(address, port).bindFlow(routes)
    bindingFuture.onComplete {
      case Success(b) =>
        println("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        println(s"Server could not bind to: $address:$port. Exception message: ${e.getMessage}")
        system.terminate()
    }
  }
}
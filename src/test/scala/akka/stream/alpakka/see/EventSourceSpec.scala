package akka.stream.alpakka.see

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorSystem, Props, Status}
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.sse.EventStreamMarshalling
import akka.http.scaladsl.model.MediaTypes.`text/event-stream`
import akka.http.scaladsl.model.StatusCodes.BadRequest
import akka.http.scaladsl.model.headers.`Last-Event-ID`
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, HttpResponse, Uri}
import akka.http.scaladsl.server.{Directives, Route}
import akka.pattern.pipe
import akka.stream.ThrottleMode
import akka.stream.alpakka.sse.scaladsl.EventSource
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.SocketUtil
import org.scalatest.{AsyncWordSpec, BeforeAndAfterAll, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

/**
  * Show the alpakka sse connector:
  * http://developer.lightbend.com/docs/alpakka/current/sse.html
  *
  * Shamelessly stolen from:
  * https://github.com/akka/alpakka/blob/master/sse/src/test/scala/akka/stream/alpakka/sse/scaladsl/EventSourceSpec.scala
  *
  * in order to show the advanced possibilities over home grown client implementations such as in SSEHeartbeat in this repo
  *
  */
object EventSourceSpec {

  final object Server {

    private final case object Bind
    private final case object Unbind

    private def route(size: Int, setEventId: Boolean): Route = {
      import Directives._
      import EventStreamMarshalling._
      get {
        optionalHeaderValueByName(`Last-Event-ID`.name) { lastEventId =>
          try {
            val fromSeqNo = lastEventId.map(_.trim.toInt).getOrElse(0) + 1
            complete {
              Source(fromSeqNo.until(fromSeqNo + size))
                .map(toServerSentEvent(setEventId))
                .intersperse(ServerSentEvent.heartbeat)
            }
          } catch {
            case _: NumberFormatException =>
              complete(
                HttpResponse(
                  BadRequest,
                  entity = HttpEntity(
                    `text/event-stream`,
                    "Integral number expected for Last-Event-ID header!".getBytes(UTF_8)
                  )
                )
              )
          }
        }
      }
    }
  }

  final class Server(address: String, port: Int, size: Int, shouldSetEventId: Boolean = false)
    extends Actor
      with ActorLogging {
    import Server._

    implicit val system = context.system
    implicit val executionContext = context.system.dispatcher

    system.scheduler.scheduleOnce(1.second, self, Bind)

    override def receive = unbound

    private def unbound: Receive = {
      case Bind =>
        Http(context.system).bindAndHandle(route(size, shouldSetEventId), address, port).pipeTo(self)
        context.become(binding)
    }

    private def binding: Receive = {
      case serverBinding @ Http.ServerBinding(socketAddress) =>
        log.info("Listening on {}", socketAddress)
        context.system.scheduler.scheduleOnce(1500.milliseconds, self, Unbind)
        context.become(bound(serverBinding))

      case Status.Failure(cause) =>
        log.error(cause, s"Can't bind to {}:{}!", address, port)
        context.stop(self)
    }

    private def bound(serverBinding: Http.ServerBinding): Receive = {
      case Unbind =>
        serverBinding.unbind().map(_ => Done).pipeTo(self)
        context.become(unbinding(serverBinding.localAddress))
    }

    private def unbinding(socketAddress: InetSocketAddress): Receive = {
      case Done =>
        log.info("Stopped listening on {}", socketAddress)
        context.system.scheduler.scheduleOnce(500.milliseconds, self, Bind)
        context.become(unbound)

      case Status.Failure(cause) =>
        log.error(cause, s"Can't unbind from {}!", socketAddress)
        context.stop(self)
    }
  }

  private def toServerSentEvent(setEventId: Boolean)(n: Int) = {
    val data = n.toString
    val event = ServerSentEvent(data)
    if (setEventId) event.copy(id = Some(data)) else event
  }

  private def hostAndPort() = {
    val address = SocketUtil.temporaryServerAddress()
    (address.getAddress.getHostAddress, address.getPort)
  }
}

final class EventSourceSpec extends AsyncWordSpec with Matchers with BeforeAndAfterAll {
  import EventSourceSpec._

  private implicit val system = ActorSystem()
  private implicit val ec = system.dispatcher

  "EventSource" should {
    "communicate correctly with an unstable HTTP server" in {
      val nrOfSamples = 20
      val (host, port) = hostAndPort()
      val server = system.actorOf(Props(new Server(host, port, 2, true)))

      //#event-source
      val eventSource = EventSource(Uri(s"http://$host:$port"), send, Some("2"), 1.second)
      //#event-source

      //#consume-events
      val events =
        eventSource.throttle(1, 500.milliseconds, 1, ThrottleMode.Shaping).take(nrOfSamples).runWith(Sink.seq)
      //#consume-events

      val expected = Seq.tabulate(nrOfSamples)(_ + 3).map(toServerSentEvent(setEventId = true))
      events.map(_ shouldBe expected).andThen { case _ => system.stop(server) }
    }

    "apply the initial last event ID if the server doesn't set the event ID" in {
      val nrOfSamples = 20
      val (host, port) = hostAndPort()
      val server = system.actorOf(Props(new Server(host, port, 2)))
      val eventSource = EventSource(Uri(s"http://$host:$port"), send, Some("2"), 1.second)
      val events = eventSource.take(nrOfSamples).runWith(Sink.seq)
      val expected = Seq.tabulate(nrOfSamples)(_ % 2 + 3).map(toServerSentEvent(setEventId = false))
      events.map(_ shouldBe expected).andThen { case _ => system.stop(server) }
    }
  }

  override protected def afterAll() = {
    Await.ready(system.terminate(), 42.seconds)
    super.afterAll()
  }

  private def send(request: HttpRequest) = Http().singleRequest(request)
}
package alpakka.tcp_to_websockets.websockets

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream._
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.requests.IsolationLevel
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.{Logger, LoggerFactory}

import java.util.Locale
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
  * Additional Kafka consumer for topic `hl7-input`,
  * which then pushes the msgs via SSE to a client
  *
  * Can be run in parallel with [[Kafka2Websocket]]
  *
  * @param mappedPortKafka
  */
class Kafka2SSE(mappedPortKafka: Int = 9092) {
  implicit val system = ActorSystem("Kafka2SSE")
  implicit val ec = system.dispatcher
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val (address, port) = ("127.0.0.1", 6000)
  server(address, port)
  backoffClient(address, port)

  val bootstrapServers = s"127.0.0.1:$mappedPortKafka"

  private def createConsumerSettings(group: String): ConsumerSettings[String, String] = {
    ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withGroupId(group)
      //Define consumer behavior upon starting to read a partition for which it does not have a committed offset or if the committed offset it has is invalid
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString.toLowerCase(Locale.ENGLISH))
  }


  private def server(address: String, port: Int) = {

    val route = {
      import akka.http.scaladsl.marshalling.sse.EventStreamMarshalling._
      import akka.http.scaladsl.server.Directives._

      def events =
        path("events" / Segment) { clientName =>
          println(s"Server received request from: $clientName")
          get {
            complete {
              Consumer
                .plainSource(createConsumerSettings("hl7-input sse consumer"), Subscriptions.topics("hl7-input"))
                .map(msg => ServerSentEvent(msg.value()))
                .keepAlive(1.second, () => ServerSentEvent.heartbeat)
            }
          }
        }
     events
    }

    val bindingFuture = Http().newServerAt(address, port).bindFlow(route)
    bindingFuture.onComplete {
      case Success(b) =>
        println("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        println(s"Server could not bind to $address:$port. Exception message: ${e.getMessage}")
        system.terminate()
    }
  }

  private def backoffClient(address: String, port: Int) = {

    import akka.http.scaladsl.unmarshalling.sse.EventStreamUnmarshalling._

    val restartSettings = RestartSettings(1.second, 10.seconds, 0.2).withMaxRestarts(10, 1.minute)
    val restartSource = RestartSource.withBackoff(restartSettings) { () =>
      Source.futureSource {
        Http()
          .singleRequest(HttpRequest(
            uri = s"http://$address:$port/events/backoffClient"
          ))
          .flatMap(Unmarshal(_).to[Source[ServerSentEvent, NotUsed]])
      }
    }

    val (killSwitch: UniqueKillSwitch, done) = restartSource
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.foreach(event => println(s"backoffClient got event: $event")))(Keep.both)
      .run()

    done.map(_ => {
      println("Reached shutdown...");
      killSwitch.shutdown()
    })
  }
}

object Kafka2SSE extends App {
  val instance = new Kafka2SSE()
  def apply(mappedPortKafka: Int) = new Kafka2SSE(mappedPortKafka)
}

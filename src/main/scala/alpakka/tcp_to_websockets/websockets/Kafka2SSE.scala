package alpakka.tcp_to_websockets.websockets

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.Http.ServerBinding
import org.apache.pekko.http.scaladsl.model.HttpRequest
import org.apache.pekko.http.scaladsl.model.sse.ServerSentEvent
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.kafka.scaladsl.Consumer
import org.apache.pekko.kafka.{ConsumerSettings, Subscriptions}
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import org.slf4j.{Logger, LoggerFactory}

import java.util.Locale
import scala.compiletime.uninitialized
import scala.concurrent.duration.*
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
  * Additional Kafka consumer for topic `hl7-input`,
  * which consumes msgs and then pushes them via SSE to a client
  *
  * Can run in parallel with [[Kafka2Websocket]]
  *
  * @param mappedPortKafka
  */
class Kafka2SSE(mappedPortKafka: Int = 9092) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val (address, port) = ("127.0.0.1", 6000)
  val bootstrapServers = s"127.0.0.1:$mappedPortKafka"

  var clientKillSwitch: UniqueKillSwitch = uninitialized
  var serverBinding: ServerBinding = uninitialized

  def run(): Unit = {
    server(address, port)
    clientKillSwitch = backoffClient(address, port)
  }

  def stop(): Unit = {
    logger.info("Stopping...")
    clientKillSwitch.shutdown()
    if (serverBinding != null) serverBinding.terminate(10.seconds) else {}
  }

  private def createConsumerSettings(group: String): ConsumerSettings[String, String] = {
    ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withGroupId(group)
      //Define consumer behavior upon starting to read a partition for which it does not have a committed offset or if the committed offset it has is invalid
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString.toLowerCase(Locale.ENGLISH))
  }


  private def server(address: String, port: Int): Unit = {

    val route = {
      import org.apache.pekko.http.scaladsl.marshalling.sse.EventStreamMarshalling.*
      import org.apache.pekko.http.scaladsl.server.Directives.*

      def events =
        path("events" / Segment) { clientName =>
          logger.info(s"Server received request from: $clientName")
          get {
            complete {
              val restartSettings = RestartSettings(1.second, 10.seconds, 0.2).withMaxRestarts(10, 1.minute)
              RestartSource.withBackoff(restartSettings) { () =>
                Consumer
                  .plainSource(createConsumerSettings("hl7-input sse consumer"), Subscriptions.topics("hl7-input"))
                  .map(msg => ServerSentEvent(msg.value()))
                  .keepAlive(1.second, () => ServerSentEvent.heartbeat)
              }
            }
          }
        }

      events
    }

    val bindingFuture = Http().newServerAt(address, port).bindFlow(route)
    bindingFuture.onComplete {
      case Success(binding) =>
        logger.info(s"Server started, listening on: {}", binding.localAddress)
        serverBinding = binding
      case Failure(e) =>
        logger.info(s"Server could not bind to: $address:$port. Exception message: ${e.getMessage}")
        system.terminate()
    }
  }

  private def backoffClient(address: String, port: Int) = {

    import org.apache.pekko.http.scaladsl.unmarshalling.sse.EventStreamUnmarshalling.*

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

    val (killSwitch: UniqueKillSwitch, _) = restartSource
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.foreach(event => logger.info(s"backoffClient got event: $event")))(Keep.both)
      .run()
    killSwitch
  }
}

object Kafka2SSE {
  lazy val instance = new Kafka2SSE()

  def main(args: Array[String]): Unit = {
    instance
    ()
  }

  def apply(mappedPortKafka: Int) = new Kafka2SSE(mappedPortKafka)
}

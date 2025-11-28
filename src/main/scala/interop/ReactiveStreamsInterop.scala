package interop

import io.reactivex.rxjava3.core.Flowable
import org.apache.camel.FluentProducerTemplate
import org.apache.camel.component.reactive.streams.api.{CamelReactiveStreams, CamelReactiveStreamsService}
import org.apache.camel.impl.DefaultCamelContext
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.ThrottleMode
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.reactivestreams.Publisher
import org.slf4j.{Logger, LoggerFactory}
import reactor.core.publisher.Flux

import java.time.Duration
import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

/**
  * Show reactive streams interop by using Apache Camel "Reactive Streams" component
  * to distribute messages to different consumers:
  *  - Reactor
  *  - RxJava
  *  - pekko-streams
  *
  * Remarks:
  *  - SEDA (Staged Event-Driven Architecture)  provides asynchronous in-memory messaging within a single CamelContext
  *
  * Doc:
  * https://doc.akka.io/docs/akka/current/stream/reactive-streams-interop.html
  * https://camel.apache.org/components/4.14.x/reactive-streams-component.html
  * https://camel.apache.org/components/4.14.x/seda-component.html
  * https://projectreactor.io/docs/core/release/reference/
  * https://github.com/ReactiveX/RxJava
  */
object ReactiveStreamsInterop extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  val camel = new DefaultCamelContext()
  val rsCamel: CamelReactiveStreamsService = CamelReactiveStreams.get(camel)
  camel.start()

  // Producer/Publisher from Camel SEDA queue using Reactive Streams
  val publisher: Publisher[String] = rsCamel.from("seda:words", classOf[String])

  // Slow consumer with Reactor 3
  Flux.from(publisher)
    .delayElements(Duration.ofMillis(2000))
    .map(each => each.toUpperCase())
    .doOnNext(each => logger.info(s"Consumed with Reactor: $each"))
    .subscribe()

  // Slow consumer with RxJava 3
  Flowable.fromPublisher(publisher)
    .delay(2L, TimeUnit.SECONDS)
    .map(each => each.toUpperCase())
    .doOnNext(each => logger.info(s"Consumed with RxJava: $each"))
    .subscribe()

  // Slow consumer with pekko-streams
  Source.fromPublisher(publisher)
    .throttle(2, 2.seconds, 2, ThrottleMode.shaping)
    .map(each => each.toUpperCase())
    .wireTap(each => logger.info(s"Consumed with pekko-streams: $each"))
    .runWith(Sink.ignore)

  val producerTemplate: FluentProducerTemplate = camel.createFluentProducerTemplate

  Source(1 to 10)
    .throttle(1, 1.seconds, 1, ThrottleMode.shaping)
    .mapAsync(1) { i =>
      producerTemplate
        .withBody(s"Camel$i")
        .to("seda:words")
        .send
      Future(i)
    }.runWith(Sink.ignore)
}

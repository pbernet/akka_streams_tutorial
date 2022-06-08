package interop

import akka.actor.ActorSystem
import akka.stream.ThrottleMode
import akka.stream.scaladsl.{Sink, Source}
import io.reactivex.rxjava3.core.Flowable
import org.apache.camel.FluentProducerTemplate
import org.apache.camel.component.reactive.streams.api.{CamelReactiveStreams, CamelReactiveStreamsService}
import org.apache.camel.impl.DefaultCamelContext
import org.reactivestreams.Publisher
import org.slf4j.{Logger, LoggerFactory}
import reactor.core.publisher.Flux

import scala.concurrent.duration.DurationInt

/**
  * Show reactive streams interop by using Apache Camel "Reactive Streams" component
  * to distribute messages to the consumers
  *
  * Doc:
  * https://doc.akka.io/docs/akka/current/stream/reactive-streams-interop.html
  * https://camel.apache.org/components/3.15.x/reactive-streams-component.html
  * https://projectreactor.io/docs/core/release/reference/
  * https://github.com/ReactiveX/RxJava
  */
object ReactiveStreamsInterop extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  implicit val system: ActorSystem = ActorSystem()

  val camel = new DefaultCamelContext()
  val rsCamel: CamelReactiveStreamsService = CamelReactiveStreams.get(camel)
  camel.start()

  val publisher: Publisher[String] = rsCamel.from("vm:words", classOf[String])

  // Consumer endpoint with Reactor 3
  Flux.from(publisher)
    .map(each => each.toUpperCase())
    .doOnNext(each => logger.info(s"Consumed with Reactor: $each"))
    .subscribe()

  // Consumer endpoint with RxJava 3
  Flowable.fromPublisher(publisher)
    .map(each => each.toUpperCase())
    .doOnNext(each => logger.info(s"Consumed with RxJava: $each"))
    .subscribe()

  // Consumer endpoint with akka-streams
  Source.fromPublisher(publisher)
    .throttle(2, 2.seconds, 2, ThrottleMode.shaping)
    .map(each => each.toUpperCase())
    .wireTap(each => logger.info(s"Consumed with akka-streams: $each"))
    .runWith(Sink.ignore)

  // Sender endpoint with Camel
  val template: FluentProducerTemplate = camel.createFluentProducerTemplate
  (1 to 10).foreach { i =>
    template
      .withBody(s"Camel$i")
      .to("vm:words")
      .send
  }
}

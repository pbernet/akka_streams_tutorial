package sample.stream_shared_state

/**
  * Drop identical (consecutive or non-consecutive) elements in an
  * unbounded stream using the squbs `Deduplicate` GraphStage:
  * https://squbs.readthedocs.io/en/latest/deduplicate
  *
  * More general than: [[DeduplicateConsecutiveElements]]
  *
  * Similar example implemented with Apache Flink:
  * https://github.com/pbernet/flink-scala-3/blob/main/src/main/scala/com/ververica/Example_05_DataStream_Deduplicate.scala
  */
// TODO Activate when "org.squbs" %% "squbs-ext" is available for pekko
object Dedupe extends App {
  //  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  //  implicit val system: ActorSystem = ActorSystem()
  //
  //  import system.dispatcher
  //
  //  val maxRandomNumber = 50
  //  // use take() for testing
  //  val sourceOfRndElements = Source.fromIterator(() => Iterator.continually(Element(ThreadLocalRandom.current().nextInt(maxRandomNumber), "payload"))).take(100)
  //
  //  val done = sourceOfRndElements
  //    .wireTap(each => logger.info(s"Before: $each"))
  //    // When duplicateCount is reached:
  //    // Remove element from internal registry/cache of already seen elements to prevent the registry growing unboundedly
  //    .via(Deduplicate((el: Element) => el.id, duplicateCount = 2))
  //    .wireTap(each => logger.info(s"After: $each"))
  //    .runWith(Sink.ignore)
  //
  //  terminateWhen(done)
  //
  //  def terminateWhen(done: Future[_]) = {
  //    done.onComplete {
  //      case Success(_) =>
  //        logger.info("Flow Success. About to terminate...")
  //        system.terminate()
  //      case Failure(e) =>
  //        logger.error(s"Flow Failure: $e. About to terminate...")
  //        system.terminate()
  //    }
  //  }
  //
  //  case class Element(id: Int, payload: String) {
  //    override def toString = s"$id"
  //  }
}

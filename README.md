[![Build Status](https://github.com/pbernet/akka_streams_tutorial/actions/workflows/ci.yml/badge.svg)](https://github.com/pbernet/akka_streams_tutorial/actions/workflows/ci.yml)
# Akka streams tutorial #

"It works!" a colleague used to shout across the office when another proof of concept was running it's first few hundred meters along the happy path, well aware that the real work started right there.
This repo contains a collection of runnable and self-contained examples from various [akka streams](https://doc.akka.io/docs/akka/current/stream/index.html) and [Alpakka](https://doc.akka.io/docs/alpakka/current/index.htmldocs) tutorials, blogs and postings to provide you with exactly this feeling.
See the class comment on how to run each example. These more complex examples are described below:
* [HTTP file download with local cache](#HTTP-file-download-with-local-cache)
* [Windturbine example](#Windturbine-example) 
* [Apache Kafka WordCount](#Apache-Kafka-WordCount)
* [HL7 V2 over TCP via Kafka to Websockets](#HL7-V2-over-TCP-via-Kafka-to-Websockets)
* [Analyse Wikipedia edits live stream](#Analyse-Wikipedia-edits-live-stream)

Most of these examples deal with some kind of (shared) state. While most akka-streams [operators](https://doc.akka.io/docs/akka/current/stream/operators/index.html) are stateless, the samples in package [sample.stream_shared_state](src/main/scala/sample/stream_shared_state) also show some trickier stateful operators in action.

Other noteworthy examples:
* The `*Echo` examples series implement round trips eg [HttpFileEcho.scala](src/main/scala/akkahttp/HttpFileEcho.scala) and [WebsocketEcho.scala](src/main/scala/akkahttp/WebsocketEcho.scala)
* Basic [gRPC examples](https://github.com/pbernet/akka_streams_tutorial/tree/grpc/src/main/scala/akka/grpc/echo) are in branch `grpc`. Use `sbt compile` or `Rebuild Project` in IDEA to re-generate the sources

Remarks:
* Requires a late JDK 11 because [caffeine 3.x](https://github.com/ben-manes/caffeine/releases) requires it as well as [ZipCryptoEcho.scala](src/main/scala/alpakka/file/ZipCryptoEcho.scala)
* Most examples are throttled, so you can see from the console output what is happening
* Some examples deliberately throw `RuntimeException`, so you can observe recovery behaviour
* The use of [testcontainers](https://www.testcontainers.org) allows running realistic scenarios (eg [SSEtoElasticsearch](src/main/scala/alpakka/sse_to_elasticsearch/SSEtoElasticsearch.scala), [KafkaServerTestcontainers](src/main/scala/alpakka/env/KafkaServerTestcontainers.scala), [SlickIT](src/test/scala/alpakka/slick/SlickIT.java))

Other resources:
* Official maintained examples are in [akka-stream-tests](https://github.com/akka/akka/tree/master/akka-stream-tests/src/test/scala/akka/stream/scaladsl), the [Streams Cookbook](https://doc.akka.io/docs/akka/current/stream/stream-cookbook.html?language=scala) and in the [Alpakka Samples](https://github.com/akka/alpakka-samples) repo
* Getting started guides: [stream-quickstart](https://doc.akka.io/docs/akka/current/stream/stream-quickstart.html) and this popular [stackoverflow article](https://stackoverflow.com/questions/35120082/how-to-get-started-with-akka-streams)
* The doc chapters [Stream composition](https://doc.akka.io/docs/akka/current/stream/stream-composition.html) and [Design Principles behind Akka Streams](https://doc.akka.io/docs/akka/current/general/stream/stream-design.html) provide useful background
* The concept of [running streams using materialized values](https://doc.akka.io/docs/akka/current/stream/stream-flows-and-basics.html#defining-and-running-streams) is also explained in this [blog](http://nivox.github.io/posts/akka-stream-materialized-values), this [video](https://www.youtube.com/watch?v=2-CK76cPB9s) and in this [stackoverflow article](https://stackoverflow.com/questions/37911174/via-viamat-to-tomat-in-akka-stream)

## HTTP file download with local cache ##
Use case with shared state:
  * Process a stream of incoming messages with reoccurring TRACE_ID
  * For the first message: download a .zip file from a `FileServer` and add TRACE_ID&rarr;Path to the local cache
  * For subsequent messages with the same TRACE_ID: fetch file from cache to avoid duplicate downloads per TRACE_ID

| Class                     | Description     |
| -------------------       |-----------------|
| [FileServer.scala](src/main/scala/alpakka/env/FileServer.scala)|Local HTTP `FileServer` for non-idempotent file download simulation|
| [LocalFileCacheCaffeine.scala](src/main/scala/sample/stream_shared_state/LocalFileCacheCaffeine.scala)|Akka streams client flow, with cache implemented with [caffeine](https://github.com/ben-manes/caffeine "")|


## Windturbine example ##
Working sample from the [blog series 1-4](http://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-iv/ "Blog 4")
 from Colin Breck where classic Actors are used to model shared state, life-cycle management and fault-tolerance in combination with akka-streams.
 Colin Breck explains these concepts and more in the 2017 Reactive Summit talk [
Islands in the Stream: Integrating Akka Streams and Akka Actors
](https://www.youtube.com/watch?v=qaiwalDyayA&list=PLKKQHTLcxDVayICsjpaPeno6aAPMCCZIz&index=4)

| Class                     | Description     |
| -------------------       |-----------------|
| [SimulateWindTurbines.scala](src/main/scala/sample/stream_actor/SimulateWindTurbines.scala)| Starts n clients which feed measurements to the server|
| [WindTurbineServer.scala](src/main/scala/sample/stream_actor/WindTurbineServer.scala)| Start server which a accumulates measurements|

 The clients communicate via websockets with the `WindTurbineServer`. After a restart of `SimulateWindTurbines` the clients are able to resume. 
 Shutting down the `WindTurbineServer` results in reporting the clients that the server is not reachable.
 After restarting `WindTurbineServer` the clients are able to resume. Since there is no persistence, the processing just continuous.


## Apache Kafka WordCount ##
The ubiquitous word count with an additional message count. A message is a sequence of words.
Start the classes in the order below and watch the console output.

| Class               | Description                                                                                                                                                                                                                                                                            |
| ------------------- |----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [KafkaServerEmbedded.scala](src/main/scala/alpakka/env/KafkaServerEmbedded.scala)| Uses [Embedded Kafka](https://github.com/embeddedkafka/embedded-kafka) (= an in-memory Kafka instance). No persistence on restart                                                                                                                                                      | 
| [WordCountProducer.scala](src/main/scala/alpakka/kafka/WordCountProducer.scala)| [akka-streams-kafka](https://doc.akka.io/docs/akka-stream-kafka/current/home.html "Doc") client which feeds random words to topic `wordcount-input`                                                                                                                                    |
| [WordCountKStreams.java](src/main/scala/alpakka/kafka/WordCountKStreams.java)| [Kafka Streams DSL](https://kafka.apache.org/documentation/streams "Doc") client to count words and messages and feed the results to `wordcount-output` and `messagecount-output` topics. Contains additional interactive queries which should yield the same results `WordCountConsumer.scala` |
| [WordCountConsumer.scala](src/main/scala/alpakka/kafka/WordCountConsumer.scala)| [akka-streams-kafka](https://doc.akka.io/docs/akka-stream-kafka/current/home.html "Doc") client which consumes aggregated results from topic `wordcount-output` and `messagecount-output`                                                                                              |
| [DeleteTopicUtil.scala](src/main/scala/alpakka/kafka/DeleteTopicUtil.scala)| Utility to reset the offset                                                                                                                                                                                                                                                            |

## HL7 V2 over TCP via Kafka to Websockets ##
This PoC in package [alpakka.tcp_to_websockets](src/main/scala/alpakka/tcp_to_websockets) is from the E-Health domain, relaying [HL7 V2](https://www.hl7.org/implement/standards/product_brief.cfm?product_id=185 "Doc") text messages in some kind of "Alpakka-Trophy" across these stages:

`Hl7TcpClient` &rarr; `Hl7Tcp2Kafka` &rarr; `KafkaServer` &rarr; `Kafka2Websocket` &rarr; `WebsocketServer`

The focus is on resilience (= try not to lose messages during the restart of the stages). However, currently messages might reach the `WebsocketServer` unordered (due to retry in  `Hl7TcpClient`) and in-flight messages may get lost (upon re-start of `WebsocketServer`).

Start each stage separate in the IDE, or together via the integration test [AlpakkaTrophySpec.scala](src/test/scala/alpakka/tcp_to_websockets/AlpakkaTrophySpec.scala)

## Analyse Wikipedia edits live stream ##
Find out whose Wikipedia articles were changed in (near) real time by tapping into the [Wikipedia Edits stream provided via SSE](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams).
The class [SSEtoElasticsearch](src/main/scala/alpakka/sse_to_elasticsearch/SSEtoElasticsearch.scala) implements a workflow, using the `title` attribute as identifier from the SSE entity to fetch the `extract` from the Wikipedia API, eg for [Douglas Adams](https://en.wikipedia.org/w/api.php?format=json&action=query&prop=extracts&exlimit=max&explaintext&exintro&titles=Douglas_Adams).
Text processing on this content using [opennlp](https://opennlp.apache.org/docs/1.9.3/manual/opennlp.html) yields `personsFound`, which are added to the `wikipediaedits` Elasticsearch index.
The index is queried periodically and the content may also be viewed with a Browser, eg

`http://localhost:{mappedPort}/wikipediaedits/_search?q=personsFound:*`

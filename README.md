[![Build Status](https://github.com/pbernet/akka_streams_tutorial/actions/workflows/ci.yml/badge.svg)](https://github.com/pbernet/akka_streams_tutorial/actions/workflows/ci.yml)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)

# Pekko tutorial #

> **_NOTE:_**  As
> of [umbrella release 22.10](https://akka.io/blog/news/2022/10/26/akka-22.10-released?_ga=2.17010235.306775319.1666799105-66127885.1666682793)
> Lightbend has changed the licensing model. [Apache Pekko](https://github.com/apache/incubator-pekko) is the open source
> alternative.
> For now the branch `migrate_pekko` contains a 1st basic migration (with a few losses). See also `migrate_pekko_grpc`.
> The plan is to move the content of those branches to a new `pekko_tutorial` repo.
> A BIG thank you to the committed Pekko committers.

"It's working!" a colleague used to shout across the office when yet another proof of concept was running it's first few
hundred
meters along the happy path, aware that the real work started right there.
This repo contains a collection of runnable and self-contained examples from
various [Akka Streams](https://doc.akka.io/docs/akka/current/stream/index.html)
and [Alpakka](https://doc.akka.io/docs/alpakka/current/index.html) tutorials, blogs and postings to provide you with
exactly this feeling.
See the class comment on how to run each example. These more complex examples are described below:
* [Element deduplication](#element-deduplication)
* [Windturbine example](#windturbine-example)
* [Apache Kafka WordCount](#apache-kafka-wordcount)
* [HL7 V2 over TCP via Kafka to Websockets](#hl7-v2-over-tcp-via-kafka-to-websockets)
* [Analyse Wikipedia edits live stream](#analyse-wikipedia-edits-live-stream)
* [Movie subtitle translation via OpenAI API](#movie-subtitle-translation-via-openai-api)

Many of the examples deal with some kind of (shared) state. While most Akka
Streams [operators](https://doc.akka.io/docs/akka/current/stream/operators/index.html) are stateless, the samples in
package [sample.stream_shared_state](src/main/scala/sample/stream_shared_state) also show some trickier stateful
operators in action.

Other noteworthy examples:
* The `*Echo` examples series implement round trips eg [HttpFileEcho](src/main/scala/akkahttp/HttpFileEcho.scala)
  and [WebsocketEcho](src/main/scala/akkahttp/WebsocketEcho)
* The branch `grpc` contains the
  basic [gRPC examples](https://github.com/pbernet/akka_streams_tutorial/tree/grpc/src/main/scala/akka/grpc/echo) and
  a [chunked file upload](https://github.com/pbernet/akka_streams_tutorial/tree/grpc/src/main/scala/akka/grpc/fileupload/FileServiceImpl.scala)
  . Use `sbt compile` or `Rebuild Project` in IDEA to re-generate the sources via the `sbt-akka-grpc` plugin.

Remarks:

* Requires a late JDK 11 because [caffeine 3.x](https://github.com/ben-manes/caffeine/releases) requires it as well
  as [ZipCryptoEcho](src/main/scala/alpakka/file/ZipCryptoEcho.scala)
* Most examples are throttled, so you can see from the console output what is happening
* Some examples deliberately throw `RuntimeException`, so you can observe recovery behaviour
* Using [testcontainers](https://www.testcontainers.org) allows running realistic scenarios (
  eg [SSEtoElasticsearch](src/main/scala/alpakka/sse_to_elasticsearch/SSEtoElasticsearch.scala)
  , [KafkaServerTestcontainers](src/main/scala/alpakka/env/KafkaServerTestcontainers.scala)
  , [SlickIT](src/test/scala/alpakka/slick/SlickIT.java))

Other resources:

* Maintained examples are
  in [akka-stream-tests](https://github.com/akka/akka/tree/main/akka-stream-tests/src/test/scala/akka/stream/scaladsl)
  , the [Streams Cookbook](https://doc.akka.io/docs/akka/current/stream/stream-cookbook.html?language=scala) and in
  the [Alpakka Samples](https://github.com/akka/alpakka-samples) repo
* Getting started guides: [stream-quickstart](https://doc.akka.io/docs/akka/current/stream/stream-quickstart.html) and
  this
  popular [stackoverflow article](https://stackoverflow.com/questions/35120082/how-to-get-started-with-akka-streams)
* The doc chapters [Stream composition](https://doc.akka.io/docs/akka/current/stream/stream-composition.html)
  and [Design Principles behind Akka Streams](https://doc.akka.io/docs/akka/current/general/stream/stream-design.html)
  provide useful background
* The concept
  of [running streams using materialized values](https://doc.akka.io/docs/akka/current/stream/stream-flows-and-basics.html#defining-and-running-streams)
  is also explained in this [blog](http://nivox.github.io/posts/akka-stream-materialized-values),
  this [video](https://www.youtube.com/watch?v=2-CK76cPB9s) and in
  this [stackoverflow article](https://stackoverflow.com/questions/37911174/via-viamat-to-tomat-in-akka-stream)

## Element deduplication ##

Dropping identical (consecutive or non-consecutive) elements in an unbounded stream:

* [DeduplicateConsecutiveElements](src/main/scala/sample/stream_shared_state/DeduplicateConsecutiveElements.scala) using
  the `sliding` operator
* [Dedupe](src/main/scala/sample/stream_shared_state/Dedupe.scala) shows
  the [squbs Deduplicate GraphStage](https://squbs.readthedocs.io/en/latest/deduplicate) which allows
  to dedupe both types

The following use case uses a local caffeine cache to avoid duplicate HTTP file downloads:

* Process a stream of incoming messages with reoccurring TRACE_ID
* For the first message: download a .zip file from a `FileServer` and add TRACE_ID&rarr;Path to the local cache
* For subsequent messages with the same TRACE_ID: fetch file from cache to avoid duplicate downloads per TRACE_ID
* Use time based cache eviction to get rid of old downloads

| Class                     | Description     |
| -------------------       |-----------------|
| [FileServer](src/main/scala/alpakka/env/FileServer.scala)|Local HTTP `FileServer` for non-idempotent file download simulation|
| [LocalFileCacheCaffeine](src/main/scala/sample/stream_shared_state/LocalFileCacheCaffeine.scala)|Akka streams client flow, with cache implemented with [caffeine](https://github.com/ben-manes/caffeine "")|

## Windturbine example ##

Working sample from
the [blog series 1-4](http://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-iv/ "Blog 4")
from Colin Breck where classic Actors are used to model shared state, life-cycle management and fault-tolerance in
combination with Akka Streams.
Colin Breck explains these concepts and more in the 2017 Reactive Summit talk [
Islands in the Stream: Integrating Akka Streams and Akka Actors
](https://www.youtube.com/watch?v=qaiwalDyayA&list=PLKKQHTLcxDVayICsjpaPeno6aAPMCCZIz&index=4)

| Class                     | Description     |
| -------------------       |-----------------|
| [SimulateWindTurbines](src/main/scala/sample/stream_actor/SimulateWindTurbines.scala)| Starts n clients which feed measurements to the server|
| [WindTurbineServer](src/main/scala/sample/stream_actor/WindTurbineServer.scala)| Start server which a accumulates measurements|

 The clients communicate via websockets with the `WindTurbineServer`. After a restart of `SimulateWindTurbines` the clients are able to resume. 
 Shutting down the `WindTurbineServer` results in reporting to the clients that the server is not reachable.
 After restarting `WindTurbineServer` the clients are able to resume. Since there is no persistence, the processing just continuous.


## Apache Kafka WordCount ##
The ubiquitous word count with an additional message count. A message is a sequence of words.
Start the classes in the order below and watch the console output.

| Class               | Description                                                                                                                                                                                                                                                                            |
| ------------------- |----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [KafkaServerEmbedded](src/main/scala/alpakka/env/KafkaServerEmbedded.scala)| Uses [Embedded Kafka](https://github.com/embeddedkafka/embedded-kafka) (= an in-memory Kafka instance). No persistence on restart                                                                                                                                                      | 
| [WordCountProducer](src/main/scala/alpakka/kafka/WordCountProducer.scala)| [akka-streams-kafka](https://doc.akka.io/docs/akka-stream-kafka/current/home.html "Doc") client which feeds random words to topic `wordcount-input`                                                                                                                                    |
| [WordCountKStreams.java](src/main/scala/alpakka/kafka/WordCountKStreams.java)| [Kafka Streams DSL](https://kafka.apache.org/documentation/streams "Doc") client to count words and messages and feed the results to `wordcount-output` and `messagecount-output` topics. Contains additional interactive queries which should yield the same results `WordCountConsumer` |
| [WordCountConsumer](src/main/scala/alpakka/kafka/WordCountConsumer.scala)| [akka-streams-kafka](https://doc.akka.io/docs/akka-stream-kafka/current/home.html "Doc") client which consumes aggregated results from topic `wordcount-output` and `messagecount-output`                                                                                              |
| [DeleteTopicUtil](src/main/scala/alpakka/kafka/DeleteTopicUtil.scala)| Utility to reset the offset                                                                                                                                                                                                                                                            |

## HL7 V2 over TCP via Kafka to Websockets ##
The PoC in package [alpakka.tcp_to_websockets](src/main/scala/alpakka/tcp_to_websockets) is from the E-Health domain, relaying [HL7 V2](https://www.hl7.org/implement/standards/product_brief.cfm?product_id=185 "Doc") text messages in some kind of "Alpakka-Trophy" across these stages:

[Hl7TcpClient](src/main/scala/alpakka/tcp_to_websockets/hl7mllp/Hl7TcpClient.scala) &rarr; [Hl7Tcp2Kafka](src/main/scala/alpakka/tcp_to_websockets/hl7mllp/Hl7Tcp2Kafka.scala) &rarr; [KafkaServer](src/main/scala/alpakka/env/KafkaServerTestcontainers.scala) &rarr; [Kafka2Websocket](src/main/scala/alpakka/tcp_to_websockets/websockets/Kafka2Websocket.scala) &rarr; [WebsocketServer](src/main/scala/alpakka/env/WebsocketServer.scala)

The focus is on resilience (= try not to lose messages during the restart of the stages). However, currently messages may reach the `WebsocketServer` unordered (due to retry in  `Hl7TcpClient`) and in-flight messages may get lost (upon re-start of `WebsocketServer`).

Start each stage separately in the IDE, or together via the integration
test [AlpakkaTrophySpec](src/test/scala/alpakka/tcp_to_websockets/AlpakkaTrophySpec.scala)

## Analyse Wikipedia edits live stream ##

Find out whose Wikipedia articles were changed in (near) real time by tapping into
the [Wikipedia Edits stream provided via SSE](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams).
The class [SSEtoElasticsearch](src/main/scala/alpakka/sse_to_elasticsearch/SSEtoElasticsearch.scala) implements a
workflow, using the `title` attribute as identifier from the SSE entity to fetch the `extract` from the Wikipedia API,
eg
for [Douglas Adams](https://en.wikipedia.org/w/api.php?format=json&action=query&prop=extracts&exlimit=max&explaintext&exintro&titles=Douglas_Adams).
Text processing on this content using [opennlp](https://opennlp.apache.org/docs/1.9.3/manual/opennlp.html)
yields `personsFound`, which are added to the `wikipediaedits` Elasticsearch index.
The index is queried periodically and the content may also be viewed with a Browser, eg

`http://localhost:{mappedPort}/wikipediaedits/_search?q=personsFound:*`

## Movie subtitle translation via OpenAI API ##

[SubtitleTranslator](src/main/scala/tools/SubtitleTranslator.scala) translates all blocks of an English
source `.srt` file to a target language using the OpenAI API endpoints:

* `/chat/completions` (gpt-3.5-turbo) used by default,
  see [Doc](https://platform.openai.com/docs/guides/chat/chat-vs-completions)
* `/completions`      (text-davinci-003) used as fallback,
  see [Doc](https://beta.openai.com/docs/api-reference/completions/create)

Akka streams helps in these areas:

* Easy workflow modelling
* Scene splitting with `session windows`. All blocks of a scene are grouped in one session and thus translated in one
  API call
* Throttling to not exceed the API rate-limits
* Continuous writing of translated blocks to the target file

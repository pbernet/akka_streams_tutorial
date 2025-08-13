[![Sourcegraph](https://img.shields.io/badge/search-this%20repo-blue)](https://sourcegraph.com/github.com/pbernet/akka_streams_tutorial "Sourcegraph")
[![NotebookLM](https://img.shields.io/badge/listen%20to-this%20repo-blue)](https://notebooklm.google.com/notebook/15ab2fde-4d85-40ed-a7cf-a6fc83223680/audio "NotebookLM")
[![GolpoAI](https://img.shields.io/badge/watch-intro-blue)](https://video.golpoai.com/share/53934a0a-8971-43b6-93cc-217f6663bf28 "GolpoAI")
[![Build Status](https://github.com/pbernet/akka_streams_tutorial/actions/workflows/ci.yml/badge.svg)](https://github.com/pbernet/akka_streams_tutorial/actions/workflows/ci.yml)
[![Scala Steward](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)

# Pekko tutorial #

This repository contains a collection of runnable and self-contained examples from
various [Pekko Streams](https://pekko.apache.org/docs/pekko/current/stream), [Pekko Connectors](https://pekko.apache.org/docs/pekko-connectors/current/)
and [Pekko HTTP](https://pekko.apache.org/docs/pekko-http/current/) tutorials, blogs, and postings.

> **Akka vs Pekko**  
> As
> of [umbrella release 22.10](https://akka.io/blog/news/2022/10/26/akka-22.10-released?_ga=2.17010235.306775319.1666799105-66127885.1666682793)
> Lightbend has changed the licensing model. [Apache Pekko](https://github.com/apache/incubator-pekko) is the open
> source
> alternative. A BIG Thank you to the committed Pekko committers.
>
> For now the branch <a href="https://github.com/pbernet/akka_streams_tutorial/tree/migrate_pekko">migrate_pekko</a>
> contains the migration (with a few losses). Currently, this is the only maintained branch.
> The plan is to move the content of this branch to a new `pekko_tutorial` repo and to support Scala 3 when Pekko
> Connectors is ready.

## Project Description

"It's working!" a colleague used to shout across the office when yet another proof of concept was running its first few
hundred meters along the happy path, aware that the real work started right there. This repo aims to provide you with
exactly this feeling by offering a collection of runnable examples.

## Getting Started

### Prerequisites

Java 17 or higher (recommended: [GraalVM](https://www.graalvm.org/downloads))

### Installation

1. Clone the repository: `git clone https://github.com/pbernet/akka_streams_tutorial.git`
2. Navigate to the project directory: `cd akka_streams_tutorial`
3. Compile the project: `sbt compile`

### Running Examples

Each example class contains instructions on how to run it from the IDE. Most examples are throttled and provide a
verbose log,
by searching the log you see what is happening. Some examples deliberately throw `RuntimeException` eg to show recovery
behaviour.

## Examples Overview

Some larger examples:
* [Element deduplication](#element-deduplication)
* [Windturbine example](#windturbine-example)
* [Apache Kafka WordCount](#apache-kafka-wordcount)
* [HL7 V2 over TCP via Kafka to Websockets](#hl7-v2-over-tcp-via-kafka-to-websockets)
* [Analyse Wikipedia edits live stream](#analyse-wikipedia-edits-live-stream)
* [Movie subtitle translation via LLMs](#movie-subtitle-translation-via-llms)

Many examples deal with shared state management. While most Pekko
Streams [operators](https://pekko.apache.org/docs/pekko/current/stream/operators/index.html) are
stateless, the samples in
package [sample.stream_shared_state](src/main/scala/sample/stream_shared_state) show some trickier stateful
operators in action.

The `*Echo` example series implement round trips eg [HttpFileEcho](src/main/scala/akkahttp/HttpFileEcho.scala)
  and [WebsocketEcho](src/main/scala/akkahttp/WebsocketEcho.scala)

Using [testcontainers](https://www.testcontainers.org) allows running realistic scenarios with just one click:

* [OIDCKeycloak](src/main/scala/akkahttp/oidc/OIDCKeycloak.scala)
* [SSEtoElasticsearch](src/main/scala/alpakka/sse_to_elasticsearch/SSEtoElasticsearch.scala)
* [ClickhousedbIT](src/test/scala/alpakka/clickhousedb/ClickhousedbIT.java)
* [InfluxdbIT](src/test/scala/alpakka/influxdb/InfluxdbIT.java)
* [SlickIT](src/test/scala/alpakka/slick/SlickIT.java)

Examples of integrating AWS services with Pekko Connectors:

* [KinesisEcho](src/main/scala/alpakka/kinesis/KinesisEcho.scala)
* [FirehoseEcho](src/main/scala/alpakka/kinesis/FirehoseEcho.scala)
* [DynamoDBEcho](src/main/scala/alpakka/dynamodb/DynamoDBEcho.scala)
* [SqsEcho](src/main/scala/alpakka/sqs/SqsEcho.scala)
* [S3Echo](src/main/scala/alpakka/s3/S3Echo.scala)

Run them via the corresponding IT test classes locally
in [localstack](https://github.com/localstack/localstack)/[minio](https://github.com/minio/minio) or against your AWS
account.

### Other example resources

* Maintained pekko-streams examples are
  in [pekko-stream-tests](https://github.com/apache/pekko/tree/main/stream-tests/src/test/scala/org/apache/pekko/stream/scaladsl)
  and the [Streams Cookbook](https://pekko.apache.org/docs/pekko/current/stream/stream-cookbook.html)
* Getting started
  guides: [Streams Quickstart Guide](https://pekko.apache.org/docs/pekko/current/stream/stream-quickstart.html#streams-quickstart-guide)
  and
  this
  popular [stackoverflow article](https://stackoverflow.com/questions/35120082/how-to-get-started-with-akka-streams)
* The doc
  chapters [Modularity, Composition and Hierarchy](https://pekko.apache.org/docs/pekko/current/stream/stream-composition.html)
  and [Design Principles behind Apache Pekko Streams](https://pekko.apache.org/docs/pekko/current/general/stream/stream-design.html)
  provide useful background
* The concept
  of [running streams using materialized values](https://pekko.apache.org/docs/pekko/current/stream/stream-flows-and-basics.html#defining-and-running-streams)
  is also explained in this [blog](http://nivox.github.io/posts/akka-stream-materialized-values),
  this [video](https://www.youtube.com/watch?v=2-CK76cPB9s) and in
  this [stackoverflow article](https://stackoverflow.com/questions/37911174/via-viamat-to-tomat-in-akka-stream)
* Maintained [Pekko Connectors examples](https://github.com/apache/pekko-connectors-samples)

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

| Class                                                                                            | Description                                                                                                 |
|--------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------|
| [FileServer](src/main/scala/alpakka/env/FileServer.scala)                                        | Local HTTP `FileServer` for non-idempotent file download simulation                                         |
| [LocalFileCacheCaffeine](src/main/scala/sample/stream_shared_state/LocalFileCacheCaffeine.scala) | Pekko streams client flow, with cache implemented with [caffeine](https://github.com/ben-manes/caffeine "") |

## Windturbine example ##

Working sample from
the [blog series 1-4](http://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-iv/ "Blog 4")
from Colin Breck where classic Actors are used to model shared state, life-cycle management and fault-tolerance in
combination with Pekko Streams.
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

| Class                                                                         | Description                                                                                                                                                                                                                                                                               |
|-------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [KafkaServerEmbedded](src/main/scala/alpakka/env/KafkaServerEmbedded.scala)   | Uses [Embedded Kafka](https://github.com/embeddedkafka/embedded-kafka) (= an in-memory Kafka instance). No persistence on restart                                                                                                                                                         | 
| [WordCountProducer](src/main/scala/alpakka/kafka/WordCountProducer.scala)     | [pekko-streams-kafka](https://pekko.apache.org/docs/pekko-connectors-kafka/current/ "Doc") client which feeds random words to topic `wordcount-input`                                                                                                                                     |
| [WordCountKStreams.java](src/main/scala/alpakka/kafka/WordCountKStreams.java) | [Kafka Streams DSL](https://kafka.apache.org/documentation/streams "Doc") client to count words and messages and feed the results to `wordcount-output` and `messagecount-output` topics. Contains additional interactive queries which should yield the same results `WordCountConsumer` |
| [WordCountConsumer](src/main/scala/alpakka/kafka/WordCountConsumer.scala)     | [pekko-streams-kafka](https://pekko.apache.org/docs/pekko-connectors-kafka/current/ "Doc") client which consumes aggregated results from topic `wordcount-output` and `messagecount-output`                                                                                               |

## HL7 V2 over TCP via Kafka to Websockets ##

The PoC in package [alpakka.tcp_to_websockets](src/main/scala/alpakka/tcp_to_websockets) is from the E-Health domain,
relaying [HL7 V2](https://www.hl7.org/implement/standards/product_brief.cfm?product_id=185 "Doc") text messages in some
kind of "Trophy" across these stages:

[Hl7TcpClient](src/main/scala/alpakka/tcp_to_websockets/hl7mllp/Hl7TcpClient.scala) &rarr; [Hl7Tcp2Kafka](src/main/scala/alpakka/tcp_to_websockets/hl7mllp/Hl7Tcp2Kafka.scala) &rarr; [KafkaServer](src/main/scala/alpakka/env/KafkaServerTestcontainers.scala) &rarr; [Kafka2Websocket](src/main/scala/alpakka/tcp_to_websockets/websockets/Kafka2Websocket.scala) &rarr; [WebsocketServer](src/main/scala/alpakka/env/WebsocketServer.scala)

The focus is on resilience (= try not to lose messages during the restart of the stages). However, currently messages may reach the `WebsocketServer` unordered (due to retry in  `Hl7TcpClient`) and in-flight messages may get lost (upon re-start of `WebsocketServer`).

Start each stage separately in the IDE, or together via the integration
test [AlpakkaTrophySpec](src/test/scala/alpakka/tcp_to_websockets/AlpakkaTrophySpec.scala)

## Analyse Wikipedia edits live stream ##

Find out whose Wikipedia articles were changed in (near) real time by tapping into
the [Wikipedia Edits stream provided via SSE](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams).
The class [WikipediaEditsAnalyser](src/main/scala/alpakka/sse_to_elasticsearch/WikipediaEditsAnalyser.scala) implements
the following workflow:

Use the `title` as identifier to fetch the `extract` from the Wikipedia API,
eg
for [Douglas Adams](https://en.wikipedia.org/w/api.php?format=json&action=query&prop=extracts&exlimit=max&explaintext&exintro&titles=Douglas_Adams).

Local NER processing on the `extract` / `content`
using [opennlp](https://opennlp.apache.org/docs/2.3.3/manual/opennlp.html)
yields `personsFoundLocal`, which are then added to the `wikipediaedits` Elasticsearch/Opensearch Index.

Also, do remote NER processing on the `extract` / `content` using OpenAI `GPT_4_O_MINI` to obtain `personsFoundRemote`.

All persons found (local and remote) can be viewed in the Index with a Browser, eg
`http://localhost:{mappedPort}/wikipediaedits/_search?q=personsFoundLocal:*`

All `content` is also transformed into embeddings using [LangChain4j](https://docs.langchain4j.dev)
`BgeSmallEnV15QuantizedEmbeddingModel` to a local
`InMemoryEmbeddingStore` to be able to RAG chat against the `content` of the currently edited Wikipedia pages via a
local AI Assistant `http://localhost:8080/assistant`

## Movie subtitle translation via LLMs ##

[SubtitleTranslator](src/main/scala/tools/SubtitleTranslator.scala) translates all blocks of an English
source `.srt` file to a target language using LLMs via [LangChain4j](https://docs.langchain4j.dev)

Pekko streams helps with:
* Workflow modelling
* Scene splitting to `session windows`. All blocks of a scene are grouped in one session and then translated in one API call
* Throttling to not exceed the [OpenAI API rate limits](https://platform.openai.com/docs/guides/rate-limits?context=tier-free)
* Continuous writing of translated blocks to the target file to avoid data loss on NW failure

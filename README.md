[![Build Status](https://travis-ci.com/pbernet/akka_streams_tutorial.svg?branch=master)](https://travis-ci.com/pbernet/akka_streams_tutorial)
# Akka streams tutorial #

"It works!" a colleague used to shout across the office when another proof of concept was running it's first few hundred meters along the happy path, well aware that the real work started right there.
This repo contains a collection of runnable and self-contained examples from various [akka streams](https://doc.akka.io/docs/akka/current/stream/index.html) docs, tutorials, blogs and postings to provide you with exactly this feeling.
See the class comment on how to run each example. These more complex examples are described below:
* [HTTP file download with local cache](#HTTP-file-download-with-local-cache)
* [Windturbine example](#Windturbine-example) 
* [Apache Kafka WordCount](#Apache-Kafka-WordCount)
* [HL7 V2 over TCP via Kafka to Websockets](#HL7-V2-over-TCP-via-Kafka-to-Websockets)

These examples all deal with some kind of shared state. 

The `*Echo` examples implement round trips eg [HttpFileEcho.scala](src/main/scala/akkahttp/HttpFileEcho.scala) and [WebsocketEcho.scala](src/main/scala/akkahttp/WebsocketEcho.scala).

Basic [gRPC examples](https://github.com/pbernet/akka_streams_tutorial/tree/grpc/src/main/scala/akka/grpc/echo) are in branch `grpc`. Use `sbt compile` or `Rebuild Project` in IDEA to re-generate the sources. 

Remarks:
* Requires JDK 8 update 252 or higher (to run akka-http 10.2.x examples in package [akkahttp](src/main/scala/akkahttp)) or a late JDK 8/11 to run [ZipCryptoEcho.scala](src/main/scala/alpakka/file/ZipCryptoEcho.scala)
* Most examples are throttled so you can see from the console output what is happening
* Some examples deliberately throw `RuntimeException`, so you can observe recovery behaviour

Other resources:
* Official maintained examples are in [akka-stream-tests](https://github.com/akka/akka/tree/master/akka-stream-tests/src/test/scala/akka/stream/scaladsl), the [Streams Cookbook](https://doc.akka.io/docs/akka/current/stream/stream-cookbook.html?language=scala) and in the [Alpakka Samples](https://github.com/akka/alpakka-samples) repo.
* Getting started guides: [stream-quickstart](https://doc.akka.io/docs/akka/current/stream/stream-quickstart.html) and this popular [stackoverflow article](https://stackoverflow.com/questions/35120082/how-to-get-started-with-akka-streams).

## HTTP file download with local cache ##
Use case with shared state:
  * Process a stream of incoming messages with reoccurring TRACE_ID
  * For the first message: download a .zip file from a `FileServer` and add TRACE_ID&rarr;Path to the local cache
  * For subsequent messages with the same TRACE_ID: fetch file from cache to avoid duplicate downloads per TRACE_ID

| Class                     | Description     |
| -------------------       |-----------------|
| [FileServer.scala](src/main/scala/alpakka/env/FileServer.scala)|Local HTTP `FileServer` for non-idempotent file download simulation|
| [LocalFileCacheCaffeine.scala](src/main/scala/sample/stream_shared_state/LocalFileCacheCaffeine.scala)|Akka streams flow, which uses a local file cache implemented with [caffeine](https://github.com/ben-manes/caffeine "") to share state|


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
The ubiquitous word count with additional message count (A message is a sequence of words).
Start the classes in the order below and watch the console output.

| Class               | Description      |
| ------------------- |-----------------|
| [KafkaServer.scala](src/main/scala/alpakka/env/KafkaServer.scala)| Standalone Kafka/Zookeeper.  
| [WordCountProducer.scala](src/main/scala/alpakka/kafka/WordCountProducer.scala)| Client which feeds words to topic `wordcount-input`. Implemented with [akka-streams-kafka](https://doc.akka.io/docs/akka-stream-kafka/current/home.html "Doc")      |
| [WordCountKStreams.java](src/main/scala/alpakka/kafka/WordCountKStreams.java)| Client which does word and message count. Implemented with [Kafka Streams DSL](https://kafka.apache.org/documentation/streams "Doc")        |
| [WordCountConsumer.scala](src/main/scala/alpakka/kafka/WordCountConsumer.scala)| Client which consumes aggregated results from topic `wordcount-output` and `messagecount-output`. Implemented with [akka-streams-kafka](https://doc.akka.io/docs/akka-stream-kafka/current/home.html "Doc")    |
| [DeleteTopicUtil.scala](src/main/scala/alpakka/kafka/DeleteTopicUtil.scala)| Utility to reset the offset    | 

`WordCountKStreams.java` and `WordCountConsumer.scala` should yield the same results.

## HL7 V2 over TCP via Kafka to Websockets ##
This PoC in package [alpakka.tcp_to_websockets](src/main/scala/alpakka/tcp_to_websockets) is some kind of Alpakka-Trophy with these stages:

`Hl7TcpClient` &rarr; `Hl7Tcp2Kafka` &rarr; `KafkaServer` &rarr; `Kafka2Websocket` &rarr; `WebsocketServer`

The focus is on resilience (= try not to lose messages during the restart of the stages). However, currently messages might reach the `WebsocketServer` unordered (due to retry in  `Hl7TcpClient`) and in-flight messages may get lost.

Start each stage separate in the IDE, or together via the integration test [AlpakkaTrophySpec.scala](src/test/scala/alpakka/tcp_to_websockets/AlpakkaTrophySpec.scala)

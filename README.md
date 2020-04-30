[![Build Status](https://travis-ci.org/pbernet/akka_streams_tutorial.svg?branch=master)](https://travis-ci.org/pbernet/akka_streams_tutorial)
# Akka streams tutorial #

"It works!" a colleague used to shout across the office when another proof of concept was running it's first few hundred meters along the happy path, well aware that the real work started right there.
This repo contains a collection of runnable and self contained examples from various akka streams docs, tutorials, blogs and postings to provide you with exactly this feeling.
See the class comment on how to run each example. These more complex examples are described below:
* HTTP file download with local cache
* Windturbine Example
* Apache Kafka WordCount
* HL7 V2 over TCP via Kafka to Websockets

These examples all deal with some kind of shared state. 

Another group of examples are the `*Echo` classes, which implement roundtrips eg in [HttpFileEcho.scala](src/main/scala/akkahttp/HttpFileEcho.scala) a HTTP file upload/download roundtrip.

Remarks:
* Most examples are throttled so you can see from the console output what is happening.
* Some examples deliberately throw `RuntimeException`, so you can observe recovery.
* No unit tests and quirky package names.

## HTTP file download with local cache ##
The example in pkg `sample.stream_shared_state` support this Use case:
  * Process a stream of incoming messages with reoccurring TRACE_ID
  * For the first message: download a .zip file from a `FileServer` and add TRACE_ID&rarr;Path to the local cache
  * For subsequent messages with the same TRACE_ID: fetch file from cache to avoid duplicate downloads per TRACE_ID

| Class                     | Description     |
| -------------------       |-----------------|
| [FileServer.scala](src/main/scala/alpakka/env/FileServer.scala)|Local HTTP `FileServer` for non-idempotent file download simulation|
| [LocalFileCacheCaffeine.scala](src/main/scala/sample/stream_shared_state/LocalFileCacheCaffeine.scala)|Akka streams flow, which uses a local file cache implemented with [caffeine](https://github.com/ben-manes/caffeine "") to share state|


## Windturbine Example ##
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
| [KafkaServer.scala](src/main/scala/kafka/KafkaServer.scala)| Standalone Kafka/Zookeeper.  
| [WordCountProducer.scala](src/main/scala/kafka/WordCountProducer.scala)| Client which feeds words to topic `wordcount-input`. Implemented with [akka-streams-kafka](https://doc.akka.io/docs/akka-stream-kafka/current/home.html "Doc")      |
| [WordCountKStreams.java](src/main/java/kafka/WordCountKStreams.java)| Client which does word and message count. Implemented with [Kafka Streams DSL](https://kafka.apache.org/documentation/streams "Doc")        |
| [WordCountConsumer.scala](src/main/scala/kafka/WordCountConsumer.scala)| Client which consumes aggregated results from topic `wordcount-output` and `messagecount-output`. Implemented with [akka-streams-kafka](https://doc.akka.io/docs/akka-stream-kafka/current/home.html "Doc")    |
| [DeleteTopicUtil.scala](src/main/scala/kafka/DeleteTopicUtil.scala)| Utility to reset the offset    | 

`WordCountKStreams.java` and `WordCountConsumer.scala` should yield the same results.

After restarting `KafkaServer`:
* `WordCountProducer` resumes feeding words
* `WordCountKStreams` resumes at the stored offset
* `WordCountConsumer` resumes at the stored offset


## HL7 V2 over TCP via Kafka to Websockets ##
PoC in package `alpakka.tcp_to_websockets` to demonstrate this path:

(`Hl7MllpSender` &rarr;) `Hl7MllpListenerAkkaStreams` &rarr; `KafkaServer` &rarr; `Kafka2Websocket` &rarr; `WebsocketServer`

Supports the restart of all parts, however in-flight messages will be lost.
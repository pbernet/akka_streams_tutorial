[![Build Status](https://travis-ci.org/pbernet/akka_streams_tutorial.svg?branch=master)](https://travis-ci.org/pbernet/akka_streams_tutorial)
# Akka streams tutorial #

A collection of simple runnable and self contained examples from various akka streams docs, tutorials and blogs.
See the class comment on how to run each example. 
Three more complex examples are described below:
* HTTP download with local file cache
* Windturbine Example
* Apache Kafka WordCount

These examples all deal with some kind of shared mutable state.

## HTTP download with local file cache ##
Taken from a real world use case:
  * Process a stream of messages with reoccurring TRACE_ID
  * For the first message: download a .zip file from a FileServer and add TRACE_ID->Path to the local cache
  * For subsequent messages with the same TRACE_ID: fetch from cache to avoid duplicate downloads per TRACE_ID

| Class                     | Description     |
| -------------------       |-----------------|
| [FileServer.scala](src/main/scala/alpakka/env/FileServer.scala)|Dummy HTTP FileServer for non-idempotent file download simulation|
| [LocalFileCacheCaffeine.scala](src/main/scala/sample/stream_shared_state/LocalFileCacheCaffeine.scala)|Akka streams Flow, which uses a local file cache implemented with [caffeine](https://github.com/ben-manes/caffeine "") to share state|


## Windturbine Example ##
Working sample from the [blog series 1-4](http://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-iv/ "Blog 4")
 from Colin Breck where Actors are used to model shared mutable state, life-cycle management and fault-tolerance in combination with akka streams.
 Colin Breck explains these concepts and more in the 2017 Reactive Summit talk [
Islands in the Stream: Integrating Akka Streams and Akka Actors
](https://www.youtube.com/watch?v=qaiwalDyayA&list=PLKKQHTLcxDVayICsjpaPeno6aAPMCCZIz&index=4)

| Class                     | Description     |
| -------------------       |-----------------|
| [SimulateWindTurbines.scala](src/main/scala/sample/stream_actor/SimulateWindTurbines.scala)| Starts n clients which feed measurements to the server|
| [WindTurbineServer.scala](src/main/scala/sample/stream_actor/WindTurbineServer.scala)| Start server which a accumulates measurements|

 The clients communicate via websockets with the _WindTurbineServer_. After a restart of _SimulateWindTurbines_ the clients are able to resume. 
 Shutting down the _WindTurbineServer_ results in reporting the clients that the server is not reachable.
 After restarting _WindTurbineServer_ the clients are able to resume. Since there is no persistence, the processing just continuous.


## Apache Kafka WordCount ##
The ubiquitous word (and message) count. Start the classes in the order below and watch the console output.

| Class               | Description      |
| ------------------- |-----------------|
| [KafkaServer.scala](src/main/scala/kafka/KafkaServer.scala)| Standalone Kafka/Zookeeper. Alternative: [Setup Kafka server manually](https://kafka.apache.org/quickstart "Instruction")  
| [WordCountProducer.scala](src/main/scala/kafka/WordCountProducer.scala)| Client which feeds words to topic _wordcount-input_. Implemented with [akka-streams-kafka](https://doc.akka.io/docs/akka-stream-kafka/current/home.html "Doc")      |
| [WordCountKStreams.scala](src/main/java/kafka/WordCountKStreams.java)| Client which does word and message count. Implemented with [Kafka Streams DSL](https://kafka.apache.org/documentation/streams "Doc")        |
| [WordCountConsumer.scala](src/main/scala/kafka/WordCountConsumer.scala)| Client which consumes aggregated results from topic _wordcount-output_ and _messagecount-output_. Implemented with [akka-streams-kafka](https://doc.akka.io/docs/akka-stream-kafka/current/home.html "Doc")    |
| [DeleteTopicUtil.scala](src/main/scala/kafka/DeleteTopicUtil.scala)| Utility to reset the offset    | 

_WordCountKStreams.java_ and _WordCountConsumer.scala_ should yield the same results.

The clients communicate via a binary protocol over TCP with the _KafkaServer_. After restarting _KafkaServer_:
* _WordCountProducer_ resumes feeding words
* _WordCountKStreams_ resumes at the stored offset
* _WordCountConsumer_ resumes at the stored offset

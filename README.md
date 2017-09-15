# Akka streams tutorial #

This is a collection of simple runnable self contained examples from various akka streams docs, tutorials and blogs. 
Two more complex examples are worth mentioning:
* Windturbine Example
* Apache Kafka WordCount


## Windturbine Example in pkg _sample.stream_actor_ ##
Working sample from the [blog series 1-3](http://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-iv/ "Blog 4")
 from Colin Breck where Actors are used to model mutable state, life-cycle management and fault-tolerance in combination with akka streams.

| Class                     | Description     |
| -------------------       |-----------------|
| SimulateWindTurbines.scala| Starts n clients|
| WindTurbineServer.scala   | Start server    |

 The clients communicate via websockets with the server. After a restart of SimulateWindTurbines the clients are able to resume. 
 Shutting down the WindTurbineServer results in reporting all of the clients that the server is not reachable.
 After the WindTurbineServer is restarted the clients are able to resume. 
 There is not persistence yet, so the processing just continuous.


## Apache Kafka WordCount in pkg _kafka_ ##
The ubiquitous word count. Start the classes in the order below and watch the console output.

| Class               | Description      |
| ------------------- |-----------------|
| KafkaServer.scala| Standalone Kafka/Zookeeper. Alternative: [Setup Kafka server manually](https://kafka.apache.org/quickstart "Instruction")  
| WordCountProducer.scala   | Client which feeds words to topic _wordcount-input_. Implemented with [reactive-kafka](https://github.com/akka/reactive-kafka "Doc")      |
| WordCountKStreams.java   | Client to process stateful wordcount. Implemented with built-in [Kafka Streams DSL](https://kafka.apache.org/documentation/streams "Doc")        |
| WordCountConsumer.scala   | Client which consumes aggregated results from topic _wordcount-output_. Implemented with [reactive-kafka](https://github.com/akka/reactive-kafka "Doc")    |
| DeleteTopicUtil.scala   | Utility to reset the offset    | 

The clients communicate via a binary protocol over TCP with the server. Behaviour after restart:
* WordCountProducer resumes feeding words
* WordCountKStreams resumes processing words at the stored offset and thus keeping the state
* WordCountConsumer resumes consuming at the stored offset

Shutting down the KafkaServer results in reporting all of the clients that the Broker is not available anymore.
After the KafkaServer is restarted the clients are able to resume. 

## TODOs ##
* Add persistence to the Windturbine Example
* Implement WordCountReactiveKafka with reactive-kafka, although akka-streams is not suited well for stateful processing 
* Add Apache Flink [example word count client](https://github.com/mkuthan/example-flink-kafka/blob/master/src/main/scala/example/flink/FlinkExample.scala "Example") 
* Implement the Windturbine example with Kafka 

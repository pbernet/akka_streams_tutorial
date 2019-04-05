name := "akka-streams-tutorial"

version := "1.0"

scalaVersion := "2.12.8"

val akkaVersion = "2.5.22"
val akkaHTTPVersion = "10.1.8"
val alpakkaVersion = "1.0.0"
val akkaStreamKafkaVersion =  "1.0.1"
val kafkaVersion = "2.2.0"
val activemqVersion =  "5.15.9"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  
  "com.typesafe.akka" %% "akka-http" % akkaHTTPVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHTTPVersion,

  "javax.jms" % "jms" % "1.1",
  "org.apache.activemq" % "activemq-client" % activemqVersion,
  "org.apache.activemq" % "activemq-broker" % activemqVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-jms" % alpakkaVersion,

  "com.typesafe.akka" %% "akka-stream-kafka" % akkaStreamKafkaVersion,
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,

  "com.lightbend.akka" %% "akka-stream-alpakka-sse" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-file" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-xml" % alpakkaVersion,
  
  "org.scalatest" %% "scalatest" % "3.0.6" % "test",
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion  % "test",
  "com.typesafe.play" %% "play" % "2.6.21",
  "com.geteventstore" %% "eventstore-client" % "4.1.1",
  "com.github.andyglow" %% "websocket-scala-client" % "0.2.4",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.11.3",
  "org.apache.httpcomponents" % "httpclient" % "4.5.8",
  "commons-io" % "commons-io" % "2.6",
  "org.apache.avro" % "avro" % "1.8.2",
  "com.twitter" %% "bijection-avro" % "0.9.6"
)

resolvers ++= Seq(
  "repository.jboss.org-public" at "https://repository.jboss.org/nexus/content/groups/public",
  "Mvnrepository" at "https://mvnrepository.com/artifact"
)

//see: https://github.com/sbt/sbt/issues/3618
val workaround = {
  sys.props += "packaging.type" -> "jar"
  ()
}

fork in run := true
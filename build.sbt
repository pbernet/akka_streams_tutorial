name := "akka-streams-tutorial"

version := "1.0"

scalaVersion := "2.12.9"

val akkaVersion = "2.5.24"
val akkaHTTPVersion = "10.1.9"
val alpakkaVersion = "1.1.1"
val akkaStreamKafkaVersion = "1.0.5"
val kafkaVersion = "2.3.0"
val activemqVersion =  "5.15.9"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  
  "com.typesafe.akka" %% "akka-http" % akkaHTTPVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHTTPVersion,

  "org.apache.geronimo.specs" % "geronimo-jms_1.1_spec" % "1.1.1",
  "org.apache.activemq" % "activemq-client" % activemqVersion,
  "org.apache.activemq" % "activemq-broker" % activemqVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-jms" % alpakkaVersion,

  "com.typesafe.akka" %% "akka-stream-kafka" % akkaStreamKafkaVersion,
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,

  "com.lightbend.akka" %% "akka-stream-alpakka-sse" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-file" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-xml" % alpakkaVersion,
  
  "com.typesafe.play" %% "play" % "2.6.21",
  "com.geteventstore" %% "eventstore-client" % "4.1.1",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.9.3",
  "org.apache.httpcomponents" % "httpclient" % "4.5.9",
  "commons-io" % "commons-io" % "2.6",
  "org.apache.avro" % "avro" % "1.9.1",
  "com.twitter" %% "bijection-avro" % "0.9.6",
  "com.github.blemale" %% "scaffeine" % "3.0.0" % "compile",
  "ch.qos.logback" % "logback-classic" % "1.2.3",

  "org.scalatest" %% "scalatest" % "3.0.6" % "test",
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion  % "test",
  "org.testcontainers" % "testcontainers" % "1.11.4" % "test",
  "junit" % "junit" % "4.13-beta-1"
)

resolvers += Resolver.url("repository.jboss.org-public", url("https://repository.jboss.org/nexus/content/groups/public"))

//see: https://github.com/sbt/sbt/issues/3618
val workaround = {
  sys.props += "packaging.type" -> "jar"
  ()
}

fork in run := true
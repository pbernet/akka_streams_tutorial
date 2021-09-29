name := "akka-streams-tutorial"

version := "1.0"

scalaVersion := "2.13.6"

val akkaVersion = "2.6.15"
val akkaHTTPVersion = "10.2.6"
val alpakkaVersion = "3.0.3"

val alpakkaKafkaConnector = "2.1.1"
val kafkaVersion = "2.7.0"

// Bumping to latest 5.16.x causes runtime issue:
// Scala module 2.11.4 requires Jackson Databind version >= 2.11.0 and < 2.12.0
val activemqVersion =  "5.16.0"
val testContainersVersion = "1.16.0"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4",
  // Latest is 1.0.0 but several dependencies still point to 0.9.1
  "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.1",

  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  
  "com.typesafe.akka" %% "akka-http" % akkaHTTPVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHTTPVersion,

  "org.apache.geronimo.specs" % "geronimo-jms_1.1_spec" % "1.1.1",
  "org.apache.activemq" % "activemq-client" % activemqVersion,
  "org.apache.activemq" % "activemq-broker" % activemqVersion,
  "org.apache.activemq" % "activemq-kahadb-store" % activemqVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-jms" % alpakkaVersion,
  "org.bouncycastle" % "bcprov-jdk15to18" % "1.69",

  "com.typesafe.akka" %% "akka-stream-kafka" % alpakkaKafkaConnector,
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,

  "com.lightbend.akka" %% "akka-stream-alpakka-sse" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-file" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-xml" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-ftp" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-elasticsearch" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-mqtt-streaming" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-mqtt" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-amqp" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-slick" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-csv" % alpakkaVersion,

  "ca.uhn.hapi" % "hapi-base" % "2.3",
  "ca.uhn.hapi" % "hapi-structures-v23" % "2.3",
  "ca.uhn.hapi" % "hapi-structures-v24" % "2.3",
  "ca.uhn.hapi" % "hapi-structures-v25" % "2.3",
  "ca.uhn.hapi" % "hapi-structures-v281" % "2.3",

  "org.apache.opennlp" % "opennlp-tools" % "1.9.3",

  "com.typesafe.play" %% "play" % "2.8.7",
  "com.typesafe.akka" %% "akka-serialization-jackson" % akkaVersion,

  "org.apache.httpcomponents" % "httpclient" % "4.5.13",
  "org.apache.httpcomponents" % "httpmime" % "4.5.13",
  "commons-io" % "commons-io" % "2.11.0",
  "org.apache.commons" % "commons-lang3" % "3.12.0",
  "com.twitter" %% "bijection-avro" % "0.9.7",
  // TODO Latest is 5.x but this causes bin compatibility issues with scala-java8-compat
  "com.github.blemale" %% "scaffeine" % "4.0.2",
  "ch.qos.logback" % "logback-classic" % "1.2.6",

  "org.testcontainers" % "testcontainers" % testContainersVersion,
  "org.testcontainers" % "elasticsearch" % testContainersVersion,
  "org.testcontainers" % "rabbitmq" % testContainersVersion,
  "org.testcontainers" % "kafka" % testContainersVersion,
  "org.testcontainers" % "postgresql" % testContainersVersion,
  "org.postgresql" % "postgresql" % "42.2.24",

  "org.scalatest" %% "scalatest" % "3.1.0" % Test,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion  % Test,
  "org.assertj" % "assertj-core" % "3.21.0" % Test,
  "junit" % "junit" % "4.13.1" % Test
)

resolvers += "repository.jboss.org-public" at "https://repository.jboss.org/nexus/content/groups/public"

//see: https://github.com/sbt/sbt/issues/3618
val workaround = {
  sys.props += "packaging.type" -> "jar"
  ()
}

scalacOptions += "-deprecation"
scalacOptions += "-feature"

run / fork := true
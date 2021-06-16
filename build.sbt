name := "akka-streams-tutorial"

version := "1.0"

scalaVersion := "2.13.6"

val akkaVersion = "2.6.14"
val akkaHTTPVersion = "10.2.2"
val alpakkaVersion = "3.0.1"

val alpakkaKafkaConnector = "2.1.0"
val kafkaVersion = "2.7.0"

val activemqVersion =  "5.16.0"
val testContainersVersion = "1.15.3"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.3",
  // Latest is 1.0.0 but several dependencies still point to 0.9.1
  "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.1",

  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,

  // Overrides needed for gRPC
  "com.typesafe.akka" %% "akka-discovery" % akkaVersion,
  "com.typesafe.akka" %% "akka-http2-support" % akkaHTTPVersion,

  // We need to explicitly "load" the timestamp.proto src
  // see:
  // https://github.com/thesamet/sbt-protoc/issues/144
  // https://github.com/thesamet/sbt-protoc
  "com.google.protobuf" % "protobuf-java" % "3.11.4" % "protobuf-src" intransitive(),

  //TODO Check out interesting other predef protos
  //"com.google.api.grpc" % "proto-google-common-protos" % "1.17.0" % "protobuf-src" intransitive(),

  "com.typesafe.akka" %% "akka-http" % akkaHTTPVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHTTPVersion,

  "org.apache.geronimo.specs" % "geronimo-jms_1.1_spec" % "1.1.1",
  "org.apache.activemq" % "activemq-client" % activemqVersion,
  "org.apache.activemq" % "activemq-broker" % activemqVersion,
  "org.apache.activemq" % "activemq-kahadb-store" % activemqVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-jms" % alpakkaVersion,
  "org.bouncycastle" % "bcprov-jdk15to18" % "1.67",

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

  "com.typesafe.play" %% "play" % "2.8.7",
  "com.typesafe.akka" %% "akka-serialization-jackson" % akkaVersion,

  "org.apache.httpcomponents" % "httpclient" % "4.5.13",
  "org.apache.httpcomponents" % "httpmime" % "4.5.13",
  "commons-io" % "commons-io" % "2.8.0",
  "org.apache.commons" % "commons-lang3" % "3.12.0",
  "com.twitter" %% "bijection-avro" % "0.9.7",
  "com.github.blemale" %% "scaffeine" % "4.0.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3",

  "org.testcontainers" % "testcontainers" % testContainersVersion,
  "org.testcontainers" % "elasticsearch" % testContainersVersion,
  "org.testcontainers" % "rabbitmq" % testContainersVersion,
  "org.testcontainers" % "kafka" % testContainersVersion,
  "org.testcontainers" % "postgresql" % testContainersVersion,
  "org.postgresql" % "postgresql" % "42.2.19",

  "org.scalatest" %% "scalatest" % "3.1.0" % Test,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion  % Test,
  "org.assertj" % "assertj-core" % "3.18.1" % Test,
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

fork in run := true

enablePlugins(AkkaGrpcPlugin)

// TODO enable gRPC with metadata accessible API on server
// see:
// https://doc.akka.io/docs/akka-grpc/current/buildtools/sbt.html#generating-server-
// https://doc.akka.io/docs/akka-grpc/current/server/details.html#accessing-request-metadata
//akkaGrpcCodeGeneratorSettings += "server_power_apis"
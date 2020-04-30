name := "akka-streams-tutorial"

version := "1.0"

scalaVersion := "2.12.10"

val akkaVersion = "2.6.1"
val akkaHTTPVersion = "10.1.11"
val alpakkaVersion = "2.0.0-M2"
val akkaStreamKafkaVersion = "2.0.1"
val kafkaVersion = "2.4.1"
val activemqVersion =  "5.15.11"
val streamzVersion = "0.11-RC1"
val camelVersion = "2.25.1"
val testContainersVersion = "1.12.5"

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
  //"com.lightbend.akka" %% "akka-stream-alpakka-s3" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-ftp" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-elasticsearch" % alpakkaVersion,

  "com.github.krasserm" %% "streamz-camel-akka" % streamzVersion,
  "org.apache.camel" % "camel-netty4" % camelVersion,
  "org.apache.camel" % "camel-jetty" % camelVersion,
  "org.apache.camel" % "camel-core" % camelVersion,
  "org.apache.camel" % "camel-stream" % camelVersion,
  "org.apache.camel" % "camel-mllp" % camelVersion,

  // TODO test https://camel.apache.org/components/latest/dataformats/hl7-dataformat.html
  "org.apache.camel" % "camel-hl7" % camelVersion,

  "ca.uhn.hapi" % "hapi-base" % "2.3",
  // TODO Switch to latest v28
  "ca.uhn.hapi" % "hapi-structures-v24" % "2.3",
  
  "com.typesafe.play" %% "play" % "2.8.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.10.1",
  "org.apache.httpcomponents" % "httpclient" % "4.5.9",
  "commons-io" % "commons-io" % "2.6",
  "org.apache.commons" % "commons-lang3" % "3.9",
  "org.apache.avro" % "avro" % "1.8.2",
  "com.twitter" %% "bijection-avro" % "0.9.6",
  "com.github.blemale" %% "scaffeine" % "3.0.0" % "compile",
  "ch.qos.logback" % "logback-classic" % "1.2.3",

  "org.scalatest" %% "scalatest" % "3.0.6" % "test",
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion  % "test",
  "org.testcontainers" % "testcontainers" % testContainersVersion,
  "org.testcontainers" % "elasticsearch" % testContainersVersion,
  "junit" % "junit" % "4.13-beta-1"
)

resolvers += "streamz at bintray" at "https://dl.bintray.com/streamz/maven"
resolvers += "repository.jboss.org-public" at "https://repository.jboss.org/nexus/content/groups/public"

//see: https://github.com/sbt/sbt/issues/3618
val workaround = {
  sys.props += "packaging.type" -> "jar"
  ()
}

scalacOptions += "-deprecation"

fork in run := true
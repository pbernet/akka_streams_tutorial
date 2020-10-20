name := "akka-streams-tutorial"

version := "1.0"

scalaVersion := "2.12.12"

val akkaVersion = "2.6.8"
val akkaHTTPVersion = "10.1.11"
val alpakkaVersion = "2.0.1"
val akkaStreamKafkaVersion = "2.0.4"

val kafkaVersion = "2.4.1"
val activemqVersion =  "5.15.13"
val streamzVersion = "0.13-RC1"
val camelVersion = "3.6.0"
val testContainersVersion = "1.14.3"

libraryDependencies ++= Seq(
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
  "com.lightbend.akka" %% "akka-stream-alpakka-jms" % alpakkaVersion,

  "com.typesafe.akka" %% "akka-stream-kafka" % akkaStreamKafkaVersion,
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,

  "com.lightbend.akka" %% "akka-stream-alpakka-sse" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-file" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-xml" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-ftp" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-elasticsearch" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-mqtt-streaming" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-amqp" % alpakkaVersion,

  "com.github.krasserm" %% "streamz-camel-akka" % streamzVersion,
  "org.apache.camel" % "camel-netty4" % camelVersion,
  "org.apache.camel" % "camel-jetty" % camelVersion,
  "org.apache.camel" % "camel-core" % camelVersion,
  "org.apache.camel" % "camel-stream" % camelVersion,
  "org.apache.camel" % "camel-mllp" % camelVersion,

  "ca.uhn.hapi" % "hapi-base" % "2.3",
  //Wait, wait, there is more...
  "ca.uhn.hapi" % "hapi-structures-v23" % "2.3",
  "ca.uhn.hapi" % "hapi-structures-v24" % "2.3",
  "ca.uhn.hapi" % "hapi-structures-v25" % "2.3",
  "ca.uhn.hapi" % "hapi-structures-v281" % "2.3",

  //https://github.com/akka/akka/issues/29351
  "com.typesafe.play" %% "play" % "2.8.2",
  "com.typesafe.akka" %% "akka-serialization-jackson" % akkaVersion,

  "org.apache.httpcomponents" % "httpclient" % "4.5.9",
  "commons-io" % "commons-io" % "2.7",
  "org.apache.commons" % "commons-lang3" % "3.11",
  "org.apache.avro" % "avro" % "1.8.2",
  "com.twitter" %% "bijection-avro" % "0.9.6",
  "com.github.blemale" %% "scaffeine" % "4.0.1",
  "ch.qos.logback" % "logback-classic" % "1.2.3",

  "org.scalatest" %% "scalatest" % "3.0.6" % "test",
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion  % "test",
  "org.testcontainers" % "testcontainers" % testContainersVersion,
  "org.testcontainers" % "elasticsearch" % testContainersVersion,
  "org.testcontainers" % "rabbitmq" % testContainersVersion,

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

//Usage: sbt dependencyTree
addDependencyTreePlugin

fork in run := true
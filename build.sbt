import sbt.Keys.libraryDependencies

name := "akka-streams-tutorial"

version := "1.0"

scalaVersion := "2.13.8"

val akkaVersion = "2.6.19"
val akkaHTTPVersion = "10.2.9"
val alpakkaVersion = "3.0.4"

val alpakkaKafkaConnector = "3.0.0"
val kafkaVersion = "3.1.0"

val activemqVersion = "5.16.3"
val artemisVersion = "2.21.0"
val testContainersVersion = "1.16.3"
val keycloakVersion = "17.0.0"
val sttpVersion = "3.5.2"
val influxdbVersion = "5.0.0"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4",

  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  
  "com.typesafe.akka" %% "akka-http" % akkaHTTPVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHTTPVersion,

  // sttp wraps around akka-http to allow for consice clients
  "com.softwaremill.sttp.client3" %% "core" % sttpVersion,
  "com.softwaremill.sttp.client3" %% "akka-http-backend" % sttpVersion,

  "org.apache.activemq" % "activemq-client" % activemqVersion exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "org.apache.activemq" % "activemq-broker" % activemqVersion exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "org.apache.activemq" % "activemq-kahadb-store" % activemqVersion exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "com.lightbend.akka" %% "akka-stream-alpakka-jms" % alpakkaVersion,
  "javax.jms" % "jms" % "1.1",
  "org.apache.activemq" % "artemis-jms-server" % artemisVersion,
  "org.apache.activemq" % "artemis-protocols" % artemisVersion pomOnly(),
  "org.apache.activemq" % "artemis-openwire-protocol" % artemisVersion,

  "org.bouncycastle" % "bcprov-jdk15to18" % "1.70",

  "com.typesafe.akka" %% "akka-stream-kafka" % alpakkaKafkaConnector,
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,
  "io.github.embeddedkafka" %% "embedded-kafka" % kafkaVersion,

  "com.lightbend.akka" %% "akka-stream-alpakka-sse" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-file" % alpakkaVersion,
  // With the latest sshj lib explicitly included, we get a more robust behaviour on "large" data sets in SftpEcho
  "com.hierynomus" % "sshj" % "0.32.0",
  "com.lightbend.akka" %% "akka-stream-alpakka-xml" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-ftp" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-elasticsearch" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-mqtt-streaming" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-mqtt" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-amqp" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-slick" % alpakkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-csv" % alpakkaVersion,

  "com.influxdb" %% "influxdb-client-scala" % influxdbVersion,
  "com.influxdb" % "flux-dsl" % influxdbVersion,
  "org.influxdb" % "influxdb-java" % "2.22",

  "ca.uhn.hapi" % "hapi-base" % "2.3",
  "ca.uhn.hapi" % "hapi-structures-v23" % "2.3",
  "ca.uhn.hapi" % "hapi-structures-v24" % "2.3",
  "ca.uhn.hapi" % "hapi-structures-v25" % "2.3",
  "ca.uhn.hapi" % "hapi-structures-v281" % "2.3",

  "org.apache.opennlp" % "opennlp-tools" % "1.9.4",

  "com.typesafe.play" %% "play" % "2.8.7",
  "com.typesafe.akka" %% "akka-serialization-jackson" % akkaVersion,

  "org.apache.httpcomponents" % "httpclient" % "4.5.13",
  "org.apache.httpcomponents" % "httpmime" % "4.5.13",
  "commons-io" % "commons-io" % "2.11.0",
  "org.apache.commons" % "commons-lang3" % "3.12.0",
  "com.twitter" %% "bijection-avro" % "0.9.7",

  "org.apache.camel" % "camel-core" % "3.15.0",
  "org.apache.camel" % "camel-reactive-streams" % "3.15.0",
  "io.projectreactor" % "reactor-core" % "3.4.16",
  "io.reactivex.rxjava3" % "rxjava" % "3.1.4",

  "com.github.blemale" %% "scaffeine" % "5.1.2",
  "ch.qos.logback" % "logback-classic" % "1.2.11",

  "org.testcontainers" % "testcontainers" % testContainersVersion,
  "org.testcontainers" % "elasticsearch" % testContainersVersion,
  "org.testcontainers" % "rabbitmq" % testContainersVersion,
  "org.testcontainers" % "kafka" % testContainersVersion,
  "org.testcontainers" % "postgresql" % testContainersVersion,
  "org.testcontainers" % "influxdb" % testContainersVersion,
  "com.github.dasniko" % "testcontainers-keycloak" % "2.1.2",

  // org.keycloak introduces com.fasterxml.jackson.core:jackson-core:2.12.1, which causes runtime ex
  "org.keycloak" % "keycloak-core" % keycloakVersion exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "org.keycloak" % "keycloak-adapter-core" % keycloakVersion exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "org.keycloak" % "keycloak-admin-client" % keycloakVersion,

  "org.postgresql" % "postgresql" % "42.3.3",

  "org.scalatest" %% "scalatest" % "3.1.0" % Test,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
  "org.assertj" % "assertj-core" % "3.22.0" % Test,

  "org.junit.jupiter" % "junit-jupiter-engine" % "5.8.2" % Test,
  "org.junit.jupiter" % "junit-jupiter-api" % "5.8.2" % Test,
  "org.testcontainers" % "junit-jupiter" % testContainersVersion % Test,

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

// Needed as long as this lib is in the dependencies
// https://eed3si9n.com/sbt-1.5.0
// https://www.scala-lang.org/blog/2021/02/16/preventing-version-conflicts-with-versionscheme.html
ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-java8-compat" % "always"
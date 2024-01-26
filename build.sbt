import sbt.Keys.libraryDependencies

name := "pekko-tutorial"

version := "1.0"

scalaVersion := "2.13.12"

val pekkoVersion = "1.0.2"
val pekkoHTTPVersion = "1.0.0"

val pekkoConnectorVersion = "1.0.0"
val kafkaVersion = "3.4.1"

val activemqVersion = "5.17.6"
val artemisVersion = "2.31.2"
val testContainersVersion = "1.19.4"
val keycloakVersion = "21.1.2" // stay with 21.x because of Java 11 compatibility
val sttpVersion = "3.9.0"
val influxdbVersion = "6.10.0"
val awsClientVersion = "2.20.162"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4",

  "org.apache.pekko" %% "pekko-stream" % pekkoVersion,
  "org.apache.pekko" %% "pekko-stream-typed" % pekkoVersion,
  "org.apache.pekko" %% "pekko-actor" % pekkoVersion,
  "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
  "org.apache.pekko" %% "pekko-slf4j" % pekkoVersion,

  "org.apache.pekko" %% "pekko-http" % pekkoHTTPVersion,
  "org.apache.pekko" %% "pekko-http-spray-json" % pekkoHTTPVersion,
  "org.json" % "json" % "20230227",

  // sttp wraps around akka-http to allow for concise clients
  "com.softwaremill.sttp.client3" %% "core" % sttpVersion,
  "com.softwaremill.sttp.client3" %% "pekko-http-backend" % sttpVersion,

  "org.apache.activemq" % "activemq-client" % activemqVersion exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "org.apache.activemq" % "activemq-broker" % activemqVersion exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "org.apache.activemq" % "activemq-kahadb-store" % activemqVersion exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "org.apache.pekko" %% "pekko-connectors-jms" % pekkoConnectorVersion,
  "javax.jms" % "jms" % "1.1",
  "javax.xml.bind" % "jaxb-api" % "2.3.0",
  "org.apache.activemq" % "artemis-jms-server" % artemisVersion,
  "org.apache.activemq" % "artemis-protocols" % artemisVersion pomOnly(),
  "org.apache.activemq" % "artemis-openwire-protocol" % artemisVersion,

  "org.bouncycastle" % "bcprov-jdk15to18" % "1.76",

  "org.apache.pekko" %% "pekko-connectors-kafka" % pekkoConnectorVersion,
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,
  "io.github.embeddedkafka" %% "embedded-kafka" % kafkaVersion,

  "org.apache.pekko" %% "pekko-connectors-sse" % pekkoConnectorVersion,
  "org.apache.pekko" %% "pekko-connectors-file" % pekkoConnectorVersion,
  // With the latest sshj lib explicitly included, we get a more robust behaviour on "large" data sets in SftpEcho
  "com.hierynomus" % "sshj" % "0.35.0",
  "org.apache.pekko" %% "pekko-connectors-xml" % pekkoConnectorVersion,
  "org.apache.pekko" %% "pekko-connectors-ftp" % pekkoConnectorVersion,
  "org.apache.pekko" %% "pekko-connectors-elasticsearch" % pekkoConnectorVersion,
  "org.apache.pekko" %% "pekko-connectors-mqtt-streaming" % pekkoConnectorVersion,
  "org.apache.pekko" %% "pekko-connectors-mqtt" % pekkoConnectorVersion,
  "org.apache.pekko" %% "pekko-connectors-amqp" % pekkoConnectorVersion,
  "org.apache.pekko" %% "pekko-connectors-slick" % pekkoConnectorVersion,
  "org.apache.pekko" %% "pekko-connectors-csv" % pekkoConnectorVersion,
  "org.apache.pekko" %% "pekko-connectors-s3" % pekkoConnectorVersion,

  "org.apache.pekko" %% "pekko-connectors-kinesis" % pekkoConnectorVersion,
  // Use latest. Ref in alpakka: 2.17.113
  "software.amazon.awssdk" % "kinesis" % awsClientVersion exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "software.amazon.awssdk" % "apache-client" % awsClientVersion exclude("com.fasterxml.jackson.core", "jackson-databind"),

  "org.apache.pekko" %% "pekko-connectors-sqs" % pekkoConnectorVersion,
  "software.amazon.awssdk" % "sqs" % awsClientVersion exclude("com.fasterxml.jackson.core", "jackson-databind"),

  "org.squbs" %% "squbs-ext" % "0.15.0", // not (yet) migrated to pekko

  "com.influxdb" %% "influxdb-client-scala" % influxdbVersion, // not (yet) migrated to pekko
  "com.influxdb" % "flux-dsl" % influxdbVersion,
  "org.influxdb" % "influxdb-java" % "2.23",

  "ca.uhn.hapi" % "hapi-base" % "2.3",
  "ca.uhn.hapi" % "hapi-structures-v23" % "2.3",
  "ca.uhn.hapi" % "hapi-structures-v24" % "2.3",
  "ca.uhn.hapi" % "hapi-structures-v25" % "2.3",
  "ca.uhn.hapi" % "hapi-structures-v281" % "2.3",

  "org.apache.opennlp" % "opennlp-tools" % "2.2.0",

  "com.crowdscriber.captions" %% "caption-parser" % "0.1.6",

  "com.typesafe.play" %% "play-json" % "2.9.4",
  "org.apache.pekko" %% "pekko-serialization-jackson" % pekkoVersion,

  "org.apache.httpcomponents.client5" % "httpclient5" % "5.2.1",
  "org.apache.httpcomponents.core5" % "httpcore5" % "5.2",
  "commons-io" % "commons-io" % "2.11.0",
  "org.apache.commons" % "commons-lang3" % "3.12.0",
  "com.twitter" %% "bijection-avro" % "0.9.7",

  //"io.apicurio" % "apicurio-registry-utils-serde" % "1.3.2.Final",


  "org.apache.camel" % "camel-core" % "3.20.2",
  "org.apache.camel" % "camel-reactive-streams" % "3.20.2",
  "io.projectreactor" % "reactor-core" % "3.5.4",
  "io.reactivex.rxjava3" % "rxjava" % "3.1.6",

  "com.github.blemale" %% "scaffeine" % "5.2.1",
  "ch.qos.logback" % "logback-classic" % "1.4.7",

  "org.testcontainers" % "testcontainers" % testContainersVersion,
  "org.testcontainers" % "elasticsearch" % testContainersVersion,
  "org.testcontainers" % "rabbitmq" % testContainersVersion,
  "org.testcontainers" % "kafka" % testContainersVersion,
  "org.testcontainers" % "postgresql" % testContainersVersion,
  "org.testcontainers" % "influxdb" % testContainersVersion,
  "org.testcontainers" % "toxiproxy" % testContainersVersion,
  "org.testcontainers" % "localstack" % testContainersVersion,
  "org.testcontainers" % "clickhouse" % testContainersVersion,

  "com.clickhouse" % "clickhouse-jdbc" % "0.6.0",
  "com.crobox.clickhouse" %% "client" % "1.1.4",

  "org.opensearch" % "opensearch-testcontainers" % "2.0.0",
  "com.github.dasniko" % "testcontainers-keycloak" % "2.5.0",
  "eu.rekawek.toxiproxy" % "toxiproxy-java" % "2.1.7",
  "org.testcontainers" % "junit-jupiter" % testContainersVersion % Test,
  "org.junit.jupiter" % "junit-jupiter-engine" % "5.9.2" % Test,
  "org.junit.jupiter" % "junit-jupiter-api" % "5.9.2" % Test,

  // org.keycloak introduces com.fasterxml.jackson.core:jackson-core:2.12.1, which causes runtime ex
  "org.keycloak" % "keycloak-core" % keycloakVersion exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "org.keycloak" % "keycloak-adapter-core" % keycloakVersion exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "org.keycloak" % "keycloak-admin-client" % keycloakVersion exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "org.jboss.spec.javax.ws.rs" % "jboss-jaxrs-api_2.1_spec" % "2.0.2.Final",

  "org.postgresql" % "postgresql" % "42.6.0",
  "io.zonky.test.postgres" % "embedded-postgres-binaries-bom" % "15.4.0" % Test pomOnly(),
  "io.zonky.test" % "embedded-postgres" % "2.0.4" % Test,

  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "org.apache.pekko" %% "pekko-testkit" % pekkoVersion % Test,
  "org.assertj" % "assertj-core" % "3.24.2" % Test
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

// For Scalafix basic stuff to work
ThisBuild / scalafixScalaBinaryVersion :=
  CrossVersion.binaryScalaVersion(scalaVersion.value)

libraryDependencies +=
  "ch.epfl.scala" %% "scalafix-core" % _root_.scalafix.sbt.BuildInfo.scalafixVersion % ScalafixConfig

inThisBuild(
  List(
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision
  )
)
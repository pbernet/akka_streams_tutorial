import sbt.Keys.libraryDependencies

name := "pekko-tutorial"

version := "1.0"

scalaVersion := "2.13.14"

val pekkoVersion = "1.1.0-M1"
val pekkoHTTPVersion = "1.1.0-M1"
val pekkoConnectorVersion = "1.0.2"
val pekkoConnectorKafkaVersion = "1.0.0"

val kafkaVersion = "3.7.0"
val activemqVersion = "5.18.5" // We are stuck with 5.x
val artemisVersion = "2.36.0"
val testContainersVersion = "1.20.1"
val keycloakVersion = "24.0.4"
val sttpVersion = "3.9.0"
val influxdbVersion = "7.1.0"
val awsClientVersion = "2.25.32"
val gatlingVersion = "3.11.5"
val circeVersion = "0.14.8"

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4",

  "org.apache.pekko" %% "pekko-stream" % pekkoVersion,
  "org.apache.pekko" %% "pekko-stream-typed" % pekkoVersion,
  "org.apache.pekko" %% "pekko-actor" % pekkoVersion,
  "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
  "org.apache.pekko" %% "pekko-slf4j" % pekkoVersion,

  "org.apache.pekko" %% "pekko-http" % pekkoHTTPVersion,
  // JSON (un)marshalling support for pekko-http
  "org.apache.pekko" %% "pekko-http-spray-json" % pekkoHTTPVersion,
  "org.apache.pekko" %% "pekko-http-xml" % pekkoHTTPVersion,

  // JSON (un)marshalling in Java examples
  "org.json" % "json" % "20240303",

  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,

  // sttp wraps around pekko-http to allow for concise clients
  "com.softwaremill.sttp.client3" %% "core" % sttpVersion,
  "com.softwaremill.sttp.client3" %% "pekko-http-backend" % sttpVersion,

  "org.apache.activemq" % "activemq-client" % activemqVersion,
  "org.apache.activemq" % "activemq-broker" % activemqVersion,
  "org.apache.activemq" % "activemq-kahadb-store" % activemqVersion,
  "org.apache.pekko" %% "pekko-connectors-jms" % pekkoConnectorVersion,
  "javax.jms" % "jms" % "1.1",
  "javax.xml.bind" % "jaxb-api" % "2.3.1",
  "org.apache.activemq" % "artemis-jms-server" % artemisVersion,
  "org.apache.activemq" % "artemis-protocols" % artemisVersion pomOnly(),
  "org.apache.activemq" % "artemis-openwire-protocol" % artemisVersion,

  "org.bouncycastle" % "bcprov-jdk15to18" % "1.78.1",

  "org.apache.pekko" %% "pekko-connectors-kafka" % pekkoConnectorKafkaVersion,
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,
  "io.github.embeddedkafka" %% "embedded-kafka" % kafkaVersion,

  "org.apache.pekko" %% "pekko-connectors-sse" % pekkoConnectorVersion,
  "org.apache.pekko" %% "pekko-connectors-file" % pekkoConnectorVersion,
  // With the latest sshj lib explicitly included, we get a more robust behaviour on "large" data sets in SftpEcho
  "com.hierynomus" % "sshj" % "0.38.0",
  "org.apache.pekko" %% "pekko-connectors-xml" % pekkoConnectorVersion,
  "org.apache.pekko" %% "pekko-connectors-ftp" % pekkoConnectorVersion,
  "org.apache.pekko" %% "pekko-connectors-elasticsearch" % pekkoConnectorVersion,
  "org.apache.pekko" %% "pekko-connectors-mqtt-streaming" % pekkoConnectorVersion,
  "org.apache.pekko" %% "pekko-connectors-mqtt" % pekkoConnectorVersion,
  "org.apache.pekko" %% "pekko-connectors-amqp" % pekkoConnectorVersion,
  "org.apache.pekko" %% "pekko-connectors-slick" % pekkoConnectorVersion,
  "org.apache.pekko" %% "pekko-connectors-csv" % pekkoConnectorVersion,
  "org.apache.pekko" %% "pekko-connectors-s3" % pekkoConnectorVersion,
  "org.apache.pekko" %% "pekko-connectors-dynamodb" % pekkoConnectorVersion,


  "org.apache.pekko" %% "pekko-connectors-kinesis" % pekkoConnectorVersion,
  "software.amazon.awssdk" % "kinesis" % awsClientVersion,
  "software.amazon.awssdk" % "apache-client" % awsClientVersion,

  "org.apache.pekko" %% "pekko-connectors-sqs" % pekkoConnectorVersion,
  "software.amazon.awssdk" % "sqs" % awsClientVersion,


  "com.influxdb" %% "influxdb-client-scala" % influxdbVersion,
  "com.influxdb" % "flux-dsl" % influxdbVersion,
  "org.influxdb" % "influxdb-java" % "2.24",

  "ca.uhn.hapi" % "hapi-base" % "2.3",
  "ca.uhn.hapi" % "hapi-structures-v23" % "2.3",
  "ca.uhn.hapi" % "hapi-structures-v24" % "2.3",
  "ca.uhn.hapi" % "hapi-structures-v25" % "2.3",
  "ca.uhn.hapi" % "hapi-structures-v281" % "2.3",

  "org.apache.opennlp" % "opennlp-tools" % "2.3.3",

  "org.apache.httpcomponents.client5" % "httpclient5" % "5.3.1",
  "org.apache.httpcomponents.core5" % "httpcore5" % "5.2.4",
  "commons-io" % "commons-io" % "2.16.1",
  "org.apache.commons" % "commons-lang3" % "3.12.0",
  "com.sksamuel.avro4s" %% "avro4s-core" % "4.1.2",

  "org.apache.camel" % "camel-core" % "3.20.2",
  "org.apache.camel" % "camel-reactive-streams" % "3.20.2",
  "io.projectreactor" % "reactor-core" % "3.5.4",
  "io.reactivex.rxjava3" % "rxjava" % "3.1.6",

  "com.github.blemale" %% "scaffeine" % "5.3.0",
  "ch.qos.logback" % "logback-classic" % "1.4.12",

  "org.testcontainers" % "testcontainers" % testContainersVersion,
  "org.testcontainers" % "elasticsearch" % testContainersVersion,
  "org.testcontainers" % "rabbitmq" % testContainersVersion,
  "org.testcontainers" % "kafka" % testContainersVersion,
  "org.testcontainers" % "postgresql" % testContainersVersion,
  "org.testcontainers" % "influxdb" % testContainersVersion,
  "org.testcontainers" % "toxiproxy" % testContainersVersion,
  "org.testcontainers" % "localstack" % testContainersVersion,
  "org.testcontainers" % "clickhouse" % testContainersVersion,

  "com.clickhouse" % "clickhouse-jdbc" % "0.6.1",
  "com.crobox.clickhouse" %% "client" % "1.2.2",

  "org.opensearch" % "opensearch-testcontainers" % "2.0.1",
  "com.github.dasniko" % "testcontainers-keycloak" % "3.3.1",
  "eu.rekawek.toxiproxy" % "toxiproxy-java" % "2.1.7",
  "org.testcontainers" % "junit-jupiter" % testContainersVersion % Test,
  "org.junit.jupiter" % "junit-jupiter-engine" % "5.9.2" % Test,
  "org.junit.jupiter" % "junit-jupiter-api" % "5.9.2" % Test,

  "org.keycloak" % "keycloak-core" % keycloakVersion,
  "org.keycloak" % "keycloak-adapter-core" % keycloakVersion,
  "org.keycloak" % "keycloak-admin-client" % keycloakVersion,
  "org.jboss.spec.javax.ws.rs" % "jboss-jaxrs-api_2.1_spec" % "2.0.2.Final",

  "org.postgresql" % "postgresql" % "42.7.3",
  "io.zonky.test.postgres" % "embedded-postgres-binaries-bom" % "16.4.0" % Test pomOnly(),
  "io.zonky.test" % "embedded-postgres" % "2.0.7" % Test,

  "org.scalatest" %% "scalatest" % "3.2.18" % Test,
  "org.apache.pekko" %% "pekko-testkit" % pekkoVersion % Test,
  "org.assertj" % "assertj-core" % "3.25.3" % Test,

  // https://docs.gatling.io/reference/integrations/build-tools/sbt-plugin/
  "io.gatling" % "gatling-core" % gatlingVersion,
  "io.gatling.highcharts" % "gatling-charts-highcharts" % gatlingVersion,
  "io.gatling" % "gatling-test-framework" % gatlingVersion
)

resolvers += "repository.jboss.org-public" at "https://repository.jboss.org/nexus/content/groups/public"

//see: https://github.com/sbt/sbt/issues/3618
val workaround = {
  sys.props += "packaging.type" -> "jar"
  ()
}

scalacOptions += "-deprecation"
scalacOptions += "-feature"
//https://docs.scala-lang.org/scala3/guides/migration/tooling-scala2-xsource3.html
scalacOptions += "-Xsource:3"

run / fork := true

Test / parallelExecution := false

enablePlugins(GatlingPlugin)

// Needed as long as this lib is in the dependencies
// https://eed3si9n.com/sbt-1.5.0
// https://www.scala-lang.org/blog/2021/02/16/preventing-version-conflicts-with-versionscheme.html
ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-java8-compat" % "always"

// For Scalafix basic stuff to work
libraryDependencies +=
  "ch.epfl.scala" %% "scalafix-core" % _root_.scalafix.sbt.BuildInfo.scalafixVersion % ScalafixConfig

inThisBuild(
  List(
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision
  )
)
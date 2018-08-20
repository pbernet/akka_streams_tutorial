name := "akka-streams-tutorial"

version := "1.0"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.14",
  "com.typesafe.akka" %% "akka-actor" % "2.5.14",
  "com.typesafe.akka" %% "akka-http" % "10.1.3",
  "com.lightbend.akka" %% "akka-stream-alpakka-sse" % "0.20",
  "javax.jms" % "jms" % "1.1",
  "org.apache.activemq" % "activemq-all" % "5.15.4",
  "com.lightbend.akka" %% "akka-stream-alpakka-jms" % "0.20",
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.20",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "com.typesafe.akka" %% "akka-testkit" % "2.5.14"  % "test",
  "com.typesafe.play" %% "play" % "2.6.10",
  "com.geteventstore" %% "eventstore-client" % "4.1.1",
  "com.github.andyglow" %% "websocket-scala-client" % "0.2.4",
  "org.apache.kafka" %% "kafka" % "1.1.1",
  "org.apache.kafka" % "kafka-streams" % "1.1.1",

  "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.10",
  "com.lightbend.akka" %% "akka-stream-alpakka-file" % "0.20",
  "org.apache.httpcomponents" % "httpclient" % "4.5.6",
  "commons-io" % "commons-io" % "2.5"
)

resolvers ++= Seq(
  "repository.jboss.org-public" at "https://repository.jboss.org/nexus/content/groups/public",
  "Mvnrepository" at "https://mvnrepository.com/artifact"
)

fork in run := true
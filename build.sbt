name := "akka-streams-tutorial"

version := "1.0"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.11",
  "com.typesafe.akka" %% "akka-actor" % "2.5.11",
  "com.typesafe.akka" %% "akka-http" % "10.1.1",
  "com.lightbend.akka" %% "akka-stream-alpakka-sse" % "0.20",
  "javax.jms" % "jms" % "1.1",
  "org.apache.activemq" % "activemq-all" % "5.14.4",
  "com.lightbend.akka" %% "akka-stream-alpakka-jms" % "0.20",
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.20",
  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "com.typesafe.akka" %% "akka-testkit" % "2.5.11"  % "test",
  "com.typesafe.play" %% "play" % "2.6.10",
  "com.geteventstore" %% "eventstore-client" % "4.1.1",
  "com.github.andyglow" %% "websocket-scala-client" % "0.2.4",
  "org.apache.kafka" %% "kafka" % "1.0.1",
  "org.apache.kafka" % "kafka-streams" % "1.0.1"
)

resolvers ++= Seq(
  "repository.jboss.org-public" at "https://repository.jboss.org/nexus/content/groups/public",
  "Mvnrepository" at "https://mvnrepository.com/artifact"
)

fork in run := true
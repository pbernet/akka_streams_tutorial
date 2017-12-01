name := "akka-streams-tutorial"

version := "1.0"

scalaVersion := "2.12.3"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.7",
  "com.typesafe.akka" %% "akka-actor" % "2.5.7",
  "com.typesafe.akka" %% "akka-http" % "10.0.10",
  "com.lightbend.akka" %% "akka-stream-alpakka-sse" % "0.11",
  //akka-stream-kafka officially currently only supports scala 2.11 und akka 2.4.18 - see
  //http://doc.akka.io/docs/akka-stream-kafka/current/home.html
  //But it seems to work...
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.18",
  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "com.typesafe.akka" %% "akka-testkit" % "2.5.7"  % "test",
  "com.typesafe.play" %% "play" % "2.6.0",
  "com.geteventstore" %% "eventstore-client" % "4.1.1",
  "com.github.andyglow" %% "websocket-scala-client" % "0.2.4",
  "org.apache.kafka" %% "kafka" % "1.0.0",
  "org.apache.kafka" % "kafka-streams" % "1.0.0"
)

fork in run := true
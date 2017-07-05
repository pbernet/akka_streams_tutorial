name := "akka-streams-tutorial"

version := "1.0"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.3",
  "com.typesafe.akka" %% "akka-actor" % "2.5.3",
  "com.typesafe.akka" %% "akka-http" % "10.0.9",
  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "com.typesafe.akka" %% "akka-testkit" % "2.5.3"  % "test",
  "com.typesafe.play" %% "play" % "2.6.0",
  "com.geteventstore" %% "eventstore-client" % "4.1.1",
  "com.github.andyglow" %% "websocket-scala-client" % "0.2.4"
)

fork in run := true

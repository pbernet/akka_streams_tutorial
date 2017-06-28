import scalariform.formatter.preferences._

name := "akka-stream-scala"

version := "1.1"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.3",
  "com.typesafe.akka" %% "akka-actor" % "2.5.3",
  "com.typesafe.akka" %% "akka-http" % "10.0.8",
  "com.typesafe.play" %% "play" % "2.6.0",
  "com.typesafe.akka" %% "akka-testkit" % "2.5.3"  % "test",
  "com.geteventstore" %% "eventstore-client" % "4.1.1"
)


fork in run := true

addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.10.4")
//The now built in dependencyTree task is usually enough
//https://www.scala-sbt.org/1.x/docs/sbt-1.4-Release-Notes.html#sbt-dependency-graph+is+in-sourced
//addDependencyTreePlugin

addSbtPlugin("com.lightbend.akka.grpc" % "sbt-akka-grpc" % "2.1.6")
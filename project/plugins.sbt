addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.12.1")
//The now built in dependencyTree task is usually enough
//https://www.scala-sbt.org/1.x/docs/sbt-1.4-Release-Notes.html#sbt-dependency-graph+is+in-sourced
//addDependencyTreePlugin

// https://docs.gatling.io/reference/integrations/build-tools/sbt-plugin
addSbtPlugin("io.gatling" % "gatling-sbt" % "4.10.2")
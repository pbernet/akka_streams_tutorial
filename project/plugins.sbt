//With sbt 1.3.x the cmd dependencyTree is broken, see workaround in build.sbt
//https://github.com/sbt/sbt/issues/4706
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")
import xsbti.compile

name := "anal"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "net.debasishg" %% "redisclient" % "3.4"
)

javacOptions ++= Seq(
  "-encoding", "UTF-8"
)

compileOrder := CompileOrder.JavaThenScala

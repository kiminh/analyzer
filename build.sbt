name := "cpc-anal"

version := "1.0"

scalaVersion := "2.11.8"

unmanagedBase := baseDirectory.value / "libs"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value
)

javacOptions ++= Seq(
  "-encoding", "UTF-8"
)

compileOrder := CompileOrder.JavaThenScala

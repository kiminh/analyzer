name := "cpc-anal"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value
)

javacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-Dfile.encoding", "UTF-8"
)

scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-Dfile.encoding", "UTF-8"
)

compileOrder := CompileOrder.JavaThenScala


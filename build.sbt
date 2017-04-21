name := "cpc-anal"

version := "1.0"

scalaVersion := "2.11.8"

unmanagedBase := baseDirectory.value / "libs"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "org.json4s" %% "json4s-native" % "3.2.11",
  "org.json4s" %% "json4s-jackson" % "3.2.11"
)

javacOptions ++= Seq(
  "-encoding", "UTF-8"
)

scalacOptions ++= Seq(
  "-feature"
)

compileOrder := CompileOrder.JavaThenScala

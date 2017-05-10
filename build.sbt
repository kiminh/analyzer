name := "cpc-anal"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "io.grpc" % "grpc-netty" % "1.2.0",
  "com.trueaccord.scalapb" %% "scalapb-runtime-grpc" % com.trueaccord.scalapb.compiler.Version.scalapbVersion
)

javacOptions ++= Seq(
  "-encoding", "UTF-8"
)

compileOrder := CompileOrder.JavaThenScala



PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

PB.protocVersion := "-v:com.google.protobuf:protoc:3.0.0"

PB.pythonExe := "C:\\Python27\\Python.exe"


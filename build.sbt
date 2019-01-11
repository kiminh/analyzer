version := "0.1"
name := "cpc-anal"
organization := "com.cpc"
scalaVersion := "2.11.8"
assemblyJarName in assembly := "cpc-anal_2.11-0.1.jar"
compileOrder := CompileOrder.JavaThenScala
javacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-source", "1.8",
  "-target", "1.8"
)

PB.protocVersion := "-v:com.google.protobuf:protoc:3.0.0"
PB.pythonExe := "C:/Python27/Python.exe"
PB.targets in Compile := Seq(
  PB.gens.java -> (sourceManaged in Compile).value,
  scalapb.gen(javaConversions = true) -> (sourceManaged in Compile).value
)

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.1.0" % "provided",
  "org.json4s" %% "json4s-native" % "3.5.1",
  "com.hankcs" % "hanlp" % "portable-1.3.4",
  "com.github.jurajburian" %% "mailer" % "1.2.1",
  "io.grpc" % "grpc-netty" % com.trueaccord.scalapb.compiler.Version.grpcJavaVersion,
  "com.trueaccord.scalapb" %% "scalapb-runtime-grpc" % com.trueaccord.scalapb.compiler.Version.scalapbVersion,
  "com.google.code.gson" % "gson" % "2.8.1",
  "com.alibaba" % "fastjson" % "1.2.39",
  "org.scalaj" %% "scalaj-http" % "2.3.0",
  "net.debasishg" %% "redisclient" % "3.9",
  "org.apache.commons" % "commons-math3" % "3.5",
  "commons-codec" % "commons-codec" % "1.9",
  "redis.clients" % "jedis" % "2.9.0"

)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case x => (assemblyMergeStrategy in assembly).value(x)
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("io.netty.handler.**" -> "shade.io.netty.handler.@1").inAll,
  ShadeRule.rename("io.netty.channel.**" -> "shade.io.netty.channel.@1").inAll,
  ShadeRule.rename("io.netty.util.**" -> "shade.io.netty.util.@1").inAll,
  ShadeRule.rename("io.netty.bootstrap.**" -> "shade.io.netty.bootstrap.@1").inAll,
  ShadeRule.rename("io.netty.buffer.**" -> "shade.io.netty.buffer.@1").inAll,
  ShadeRule.rename("com.google.common.**" -> "shade.com.google.common.@1").inAll,
  ShadeRule.rename("com.google.protobuf.**" -> "shade.com.google.protobuf.@1").inAll
)

assemblyExcludedJars in assembly := {
  val jars = Seq(
    "c3p0-0.9.5.2.jar",
    "config-1.2.1.jar",
    "hadoop-lzo-0.4.20.jar",
    "imlib_2.11-0.0.1.jar",
    "jedis-2.1.0.jar",
    "mariadb-java-client-1.5.9.jar",
    "mchange-commons-java-0.2.11.jar",
    "mysql-connector-java-5.1.41-bin.jar",
    "protobuf-java-3.0.2.jar",
    "spark-redis-0.3.2.jar",
    "spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar",
    "xgboost4j-0.7.jar",
    "xgboost4j-spark-0.7.jar",
    "scala-library-2.11.8.jar",
    "scala-redis_2.11-1.0.jar"
  )
  (fullClasspath in assembly).value.filter {
    x => jars.contains(x.data.getName)
  }
}


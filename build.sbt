lazy val buildSettings = Seq(
  version := "1.0",
  organization := "com.cpc",
  scalaVersion := "2.11.8"
)

val app = (project in file("")).
  settings(buildSettings: _*).
  settings(
    name := "cpc-anal",
    version := "1.0",
    scalaVersion := "2.11.8",
    compileOrder := CompileOrder.JavaThenScala,

    javacOptions ++= Seq(
      "-encoding", "UTF-8"
    ),

    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    ),

    PB.protocVersion := "-v:com.google.protobuf:protoc:3.0.0",

    PB.pythonExe := "C:\\Python27\\Python.exe",

    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % scalaVersion.value % "provided",
      "org.apache.spark" %% "spark-mllib" % "2.1.0" % "provided",
      "io.grpc" % "grpc-netty" % "1.2.0",
      "com.trueaccord.scalapb" %% "scalapb-runtime-grpc" % com.trueaccord.scalapb.compiler.Version.scalapbVersion
    ),

    assemblyMergeStrategy in assembly := {
      case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
      case x => (assemblyMergeStrategy in assembly).value(x)
    },

    assemblyShadeRules in assembly := Seq(
      ShadeRule.rename("io.grpc.**" -> "shadeio.grpc.@1").inAll
    ),

    assemblyExcludedJars in assembly := {
      val cp = (fullClasspath in assembly).value
      val jars = Seq(
        "c3p0-0.9.5.2.jar",
        "commons-codec-1.10.jar",
        "config-1.2.1.jar",
        "cpc-protocol_2.11-1.0.jar",
        "hadoop-annotations-2.7.3.jar",
        "hadoop-common-2.7.3.jar",
        "hadoop-lzo-0.4.20.jar",
        "hive-cli-1.2.1.spark2.jar",
        "jackson-annotations-2.6.5.jar",
        "jackson-core-2.6.5.jar",
        "json4s-ast_2.11-3.5.1.jar",
        "json4s-core_2.11-3.5.1.jar",
        "json4s-native_2.11-3.5.1.jar",
        "mariadb-java-client-1.5.9.jar",
        "mchange-commons-java-0.2.11.jar",
        "mysql-connector-java-5.1.41-bin.jar",
        "parquet-encoding-1.8.1.jar",
        "protobuf-java-3.0.2.jar",
        "scala-redis_2.11-1.0.jar",
        "spark-catalyst_2.11-2.1.0.jar",
        "spark-core_2.11-2.1.0.jar",
        "spark-sql_2.11-2.1.0.jar",
        "spark-streaming_2.11-2.1.0.jar",
        "spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar"
      )
      cp.filter {
        x => jars.contains(x.data.getName)
      }
    }
  )





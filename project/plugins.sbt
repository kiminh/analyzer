logLevel := Level.Warn

addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.8")

libraryDependencies += "com.trueaccord.scalapb" %% "compilerplugin" % "0.6.0-pre4"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.4")

//resolvers += Resolver.url("bintray-sbt-plugins", url("https://dl.bintray.com/sbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)


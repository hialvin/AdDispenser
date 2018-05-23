name := "WordSeeker"

version := "0.1"

scalaVersion := "2.10.6"
scalacOptions += "-Ylog-classpath"

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
resolvers += "ClouderaRepo" at "https://repository.cloudera.com/content/repositories/releases"

libraryDependencies += "org.scala-sbt" %% "compiler-bridge" % "1.1.5" % Test

//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0-cdh5.13.1"

//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.0-cdh5.13.1" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
//libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.6"
//libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.6"
//libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.6"
//libraryDependencies += "org.apache.hbase" % "hbase-hadoop-compat" % "1.2.6"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.0-cdh5.13.1"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.0-cdh5.13.1"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.0-cdh5.13.1"
libraryDependencies += "org.apache.hbase" % "hbase-hadoop-compat" % "1.2.0-cdh5.13.1"
libraryDependencies += "org.apache.hbase" % "hbase-spark" % "1.2.0-cdh5.13.1"

//libraryDependencies += "org.apache.hbase" % "hbase-spark" % "2.0.0-alpha4"


libraryDependencies += "RedisLabs" % "spark-redis" % "0.3.2"
libraryDependencies ++= Seq(
  "net.debasishg" %% "redisclient" % "3.6"
)

val json4sNative = "org.json4s" %% "json4s-native" % "{latestVersion}"
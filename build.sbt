name := "Brewery"

version := "1.0"


fork := true

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "2.2.0"

libraryDependencies += 	"org.mongodb.spark" % "mongo-spark-connector_2.11" % "2.2.0"


javaOptions in run += "-XX:MaxPermSize=128M"

scalacOptions += "-target:jvm-1.8"

scalacOptions ++= Seq("-unchecked", "-deprecation")
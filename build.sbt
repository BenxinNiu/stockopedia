name := "stockopedia"

version := "0.1"


//libraryDependencies += "org.typelevel" %% "cats" % "0.9.0"

mainClass in (Compile, run) := Some("Entry.DailyEntry")

fork in run := true

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "2.0.0",
  "org.apache.spark" % "spark-sql_2.10" % "2.0.0",
  "org.apache.hadoop" % "hadoop-common" % "2.7.1",
  "com.typesafe" % "config" % "1.2.1",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.4.0",
  "org.mongodb" %% "casbah" % "3.1.1"


)


version := "0.1"
scalaVersion := "2.13.9"

name := "ModifiedWordCount"

val provided = "provided"
val hdp = ("org.apache.hadoop", "2.10.1")
libraryDependencies ++= Seq(
  hdp._1 % "hadoop-common" % hdp._2 % provided,
  hdp._1 % "hadoop-hdfs" % hdp._2 % provided,
  hdp._1 % "hadoop-mapreduce-client-core" % hdp._2 % provided
)
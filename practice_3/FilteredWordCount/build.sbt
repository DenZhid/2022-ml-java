version := "0.1"

scalaVersion := "2.11.12"

name := "filtered-word-count"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8"
)
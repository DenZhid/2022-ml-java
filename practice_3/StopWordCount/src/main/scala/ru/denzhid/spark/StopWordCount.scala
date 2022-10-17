package ru.denzhid.spark

import org.apache.spark.SparkContext

object StopWordCount {
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val stopWords:List[String] = args(2).split(",").toList

    val sc = new SparkContext()

    sc.textFile(input)
      .flatMap(line => line.split("\\s"))
      .filter(word => !stopWords.contains(word))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy { case (_, count) => count }
      .saveAsTextFile(output)

    sc.stop()
  }
}

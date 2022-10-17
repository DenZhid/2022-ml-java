package ru.denzhid.spark

import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)

    val sc = new SparkContext()

    sc.textFile(input)
      .flatMap(line => line.split("\\s"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy { case (_, count) => count }
      .saveAsTextFile(output)

    sc.stop()
  }
}

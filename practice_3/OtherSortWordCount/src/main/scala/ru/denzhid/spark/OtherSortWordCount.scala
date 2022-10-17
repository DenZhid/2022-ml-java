package ru.denzhid.spark

import org.apache.spark.SparkContext

object OtherSortWordCount {

  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)

    val sc = new SparkContext()

    sc.textFile(input)
      .flatMap(line => line.split("\\s"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(entity => (entity._2, entity._1))
      .sortBy { case (count, _) => count }
      .saveAsTextFile(output)

    sc.stop()
  }
}

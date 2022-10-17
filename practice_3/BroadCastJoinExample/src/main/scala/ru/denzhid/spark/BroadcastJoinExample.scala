package ru.denzhid.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object BroadcastJoinExample {

  def main(args: Array[String]): Unit = {
    val inputOfSmall = args(0)
    val inputOfBig = args(1)
    val output = args(2)

    val sc = new SparkContext()

    val rdd1 = mapFile(sc, inputOfBig)

    val rdd2 = mapFile(sc, inputOfSmall)
      .collect()
      .toMap

    val broadcasted = sc.broadcast(rdd2)

    rdd1.flatMap(x => {
      broadcasted.value.get(x._2) match {
        case Some(r) => Some(x._2, (x._1, r))
        case None => None
      }
    }).saveAsTextFile(output)

    sc.stop()
  }

  private def mapFile(sc: SparkContext, input: String): RDD[(String, String)] = {
    sc.textFile(input)
      .map(line => {
        val entities = line.split(",")
        (entities(0), entities(1))
      })
  }
}

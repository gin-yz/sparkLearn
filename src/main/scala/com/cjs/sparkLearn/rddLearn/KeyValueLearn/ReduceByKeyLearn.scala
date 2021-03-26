package com.cjs.sparkLearn.rddLearn.KeyValueLearn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object ReduceByKeyLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupByKey").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd1: RDD[(Int, Int)] = sc.parallelize(Array((1, 100), (1, 200), (2, 3000),(3, 5000), (4, 1),(4, 2)),4)

    val value: RDD[(Int, Int)] = rdd1.reduceByKey((a, b) => a + b,2)

    val value1: RDD[(Int, Int)] = rdd1.foldByKey(100) {
      _ + _
    }

    println(rdd1.glom().map(_.toList).collect().toList)
    println(value.glom().map(_.toList).collect().toList)
    println(value.collect().toList)
    println(value1.collect().toList)
  }

}

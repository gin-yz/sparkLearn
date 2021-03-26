package com.cjs.sparkLearn.rddLearn.KeyValueLearn

import org.apache.spark.{SparkConf, SparkContext}

object SortByKVLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SortByKVLearn").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    val value = rdd1.sortBy(_._2,ascending = false)

    println(value.collect().toList)
  }

}

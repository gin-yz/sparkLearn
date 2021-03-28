package com.cjs.sparkLearn.rddLearn.PartitionLearn

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object HashPartiotionLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupByLearn").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd1: RDD[(Int, String)] = sc.parallelize(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (4, "ddd")), 4)

    val rdd2: RDD[(Int, String)] = rdd1.partitionBy(new HashPartitioner(2)) //指定分区数

    println(rdd2.glom().map(_.toList).collect().mkString(", "))
    //    rdd2.saveAsTextFile("output")
  }

}

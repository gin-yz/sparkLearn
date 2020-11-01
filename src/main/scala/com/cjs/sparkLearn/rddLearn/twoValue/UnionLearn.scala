package com.cjs.sparkLearn.rddLearn.twoValue

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object UnionLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupByLearn").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(1 to 16)

    val rdd2: RDD[Int] = sc.makeRDD(8 to 24)

    val value: RDD[Int] = rdd1.union(rdd2)
    println(value.collect().toList) //相同元素只出现一次
  }

}

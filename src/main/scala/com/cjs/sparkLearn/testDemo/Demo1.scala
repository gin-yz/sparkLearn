package com.cjs.sparkLearn.testDemo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Demo1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CombineByKeyLearn").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input: RDD[(String, Int)] = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)

    val value: RDD[(String, Int)] = input.combineByKey(x => x, (x: Int, y: Int) => x + y, (x: Int, y: Int) => x + y)

    println(value.collect().toList)
  }
}

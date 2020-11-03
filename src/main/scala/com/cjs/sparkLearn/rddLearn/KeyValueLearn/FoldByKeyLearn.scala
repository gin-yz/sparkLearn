package com.cjs.sparkLearn.rddLearn.KeyValueLearn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FoldByKeyLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupByKey").setMaster("local[*]")

    val sc = new SparkContext(conf)
    val rdd: RDD[(Int, Int)] = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)

    val value: RDD[(Int, Int)] = rdd.foldByKey(0)(_+_)

    println(value.collect().mkString(", "))
  }

}

package com.cjs.sparkLearn.rddLearn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FilterLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupByLearn").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val value: RDD[String] = sc.makeRDD(Array("xiaoming", "xiaojiang", "xiaohe", "dazhi"))

    val value1: RDD[String] = value.filter(_.contains("xiao"))

    println(value1.collect().toList)
  }

}

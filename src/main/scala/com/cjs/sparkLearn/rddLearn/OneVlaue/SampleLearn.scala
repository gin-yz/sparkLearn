package com.cjs.sparkLearn.rddLearn.OneVlaue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SampleLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupByLearn").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.makeRDD(1 to 16)

    val value1: RDD[Int] = value.sample(withReplacement = true, fraction = 0.5, seed = 428)

    println(value1.collect().toList)
  }

}

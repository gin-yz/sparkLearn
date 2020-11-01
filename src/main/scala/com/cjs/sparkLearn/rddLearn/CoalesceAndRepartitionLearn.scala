//coalesce用于缩减对应分区,repartition本质上是调用coalesce,
package com.cjs.sparkLearn.rddLearn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CoalesceAndRepartitionLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CoalesceLearn").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.makeRDD(1 to 16,8)
    println(value.getNumPartitions)

    val value1: RDD[Int] = value.coalesce(4, shuffle = true) //suffle参数用于指定是否洗牌
    val value2: RDD[Int] = value.repartition(4) //repartition本质上是调用coalesce

    println(value1.getNumPartitions)
    println(value2.getNumPartitions)


  }

}

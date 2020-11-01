package com.cjs.sparkLearn.rddLearn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FlatMapLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FlatMapLearn")

    val sc = new SparkContext(conf)

    val value: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2, 3), List(4, 5, 6)))

    //不管是什么，转换成List就可以，之后flatMap会自动将List拆分
    val value1: RDD[Int] = value.flatMap(datas => datas)

    value1.foreach(println)

    val value2: RDD[Int] = sc.makeRDD(1 to 5)
    val value3: RDD[Int] = value2.flatMap(1 to _)
    println(value3.collect().toList)
  }

}

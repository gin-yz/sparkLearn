package com.cjs.sparkLearn.rddLearn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortByLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SortByLearn")

    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.makeRDD(1 to 16)

    //依照排好序的组进行排列,结果(2, 5, 8, 11, 14, 1, 4, 7, 10, 13, 16, 3, 6, 9, 12, 15)
    val value1: RDD[Int] = value.sortBy(_ % 3, ascending = false)

    //使用glom将分区分组
    val value2: RDD[List[Int]] = value.sortBy(_ % 3).glom().filter(!_.isEmpty).map(_.toList)

    println(value1.collect().toList)
    println(value2.collect().mkString(", "))
    println(value.sortBy(x=>x,ascending = false).collect().toList)//只降序的排一次
  }

}

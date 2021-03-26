package com.cjs.sparkLearn.initLearn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DemoTest2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcountdemo") //local[*]

    val sc = new SparkContext(conf)

    val list: List[(String, Int)] = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    val input: RDD[(String, Int)] = sc.makeRDD(list, 2)

    println(input.groupByKey().map(item => (item._1, item._2.toList.sum / item._2.toList.size)).collect().toList)

    sc.stop()


  }

}

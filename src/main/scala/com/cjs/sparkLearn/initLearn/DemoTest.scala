package com.cjs.sparkLearn.initLearn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DemoTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcountdemo") //local[*]

    val sc = new SparkContext(conf)

    val value:RDD[String] = sc.textFile(DemoTest.getClass.getResource("./createRDD.txt").getFile,2)

    val stringToLong = value.flatMap(_.split(" ")).countByValue()

    println(stringToLong.toList)

//    val value1:RDD[(Int,Int)] = value.mapPartitionsWithIndex((index, x) => x.map((index, _)))

//    value1.foreach(println(_))

    sc.stop()
  }
}

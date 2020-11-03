package com.cjs.sparkLearn.exampleDo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AggregateLearn")

    val sc = new SparkContext(conf)


    val rdd1: RDD[String] = sc.textFile(Test1.getClass.getResource("./agent.log").toString)

    val rdd2: RDD[((String, String), Int)] = rdd1.map(value => {
      val strings: Array[String] = value.split(" ")

      ((strings.apply(1), strings.apply(3)), 1)
    })

    val rdd3: RDD[((String, String), Int)] = rdd2.reduceByKey(_ + _)

    val rdd4: RDD[(String, (String, Int))] = rdd3.map { case ((city, ad), count) => (city, (ad, count)) }

    val rdd5: RDD[(String, Iterable[(String, Int)])] = rdd4.groupByKey()

    val rdd6: RDD[(String, List[(String, Int)])] = rdd5.mapValues(items => items.toList.sortWith((item1, item2) => item1._2 > item2._2).take(3))

    println(rdd6.collect().toList)



  }

}

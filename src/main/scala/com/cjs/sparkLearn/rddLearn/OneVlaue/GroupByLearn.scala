package com.cjs.sparkLearn.rddLearn.OneVlaue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupByLearn {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupByLearn").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.makeRDD(1 to 16, 8)

    val value2 = value.glom()
    println(value2.collect().map(_.toList).toList)

    val value1: RDD[(Int, Iterable[Int])] = value.groupBy(_ % 2)

    value1.saveAsTextFile("./output")

    println(value1.glom().map(_.toList).collect().toList)

    value1.foreach(println)


  }

}

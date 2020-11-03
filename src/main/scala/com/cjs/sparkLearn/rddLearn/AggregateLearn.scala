//将两个分片的先加再相乘
package com.cjs.sparkLearn.rddLearn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object AggregateLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AggregateLearn")

    val sc = new SparkContext(conf)

    val rdd: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2, 3), List(4, 5, 6)), 2)

    //value1的初始值为zeroValue为１，会参与到接下来的两个运算中的初始值，在加的时候，不要，在乘的时候，为１
    val i: Int = rdd.aggregate(1)((value1, value2) => value2.sum, (value1, value2) => value1 * value2)

    println(i)

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    //将分区转为两个list后再操作
    val value: RDD[List[Int]] = rdd1.glom().map(_.toList)

    val i1: Int = value.aggregate(1)((value1, value2) => value2.sum, (value1, value2) => value1 * value2)

    println(i1)

  }

}

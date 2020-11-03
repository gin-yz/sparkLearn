//会经历ｓｕｆｆｌｅ过程
package com.cjs.sparkLearn.rddLearn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object DistinctLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DistinctLearn")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.makeRDD(List(1, 1, 1, 2, 2, 3, 4, 5, 6, 6, 10, 10))

    val value1: RDD[Int] = value.distinct() //distinct中可接参数，表示分区,对应最后生成的文件数,若不写，local[]数

    value1.saveAsTextFile("output")
  }

}

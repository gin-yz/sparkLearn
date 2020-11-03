//在类型为(K,V)和(K,W)的 RDD 上调用，返回一个相同 key 对应的所有元素对在一起的(K,(V,W))的 RDD
package com.cjs.sparkLearn.rddLearn.KeyValueLearn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object JoinLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[(Int, String)] = sc.makeRDD(List((1, "cjs"),(1, "cjs0"), (2, "cjs1"), (3, "cjs2")))

    val rdd2: RDD[(Int, String)] = sc.makeRDD(List((1, "dsg"), (1, "dsg0"),(2, "dsg1"), (3, "dsg2")))

    val value: RDD[(Int, (String, String))] = rdd1.join(rdd2)

    println(value.collect().toList)

  }
}

//在类型为(K,V)和(K,W)的 RDD 上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的 RDD
package com.cjs.sparkLearn.rddLearn.KeyValueLearn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CogrooupLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[(Int, String)] = sc.makeRDD(List((1, "cjs"), (1, "cjs0"), (2, "cjs1"), (3, "cjs2")))

    val rdd2: RDD[(Int, String)] = sc.makeRDD(List((1, "dsg"), (1, "dsg0"), (2, "dsg1"), (3, "dsg2")))

    val value: RDD[(Int, (Iterable[String], Iterable[String]))] = rdd1.cogroup(rdd2)

    println(value.collect().toList)
  }

}

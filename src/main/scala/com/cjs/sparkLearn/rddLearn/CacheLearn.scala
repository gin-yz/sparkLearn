/*
* 两个RDD之间增加缓存，防止后面rdd失效,可以随时从此cache点读
* persist和cache方法，cache方法是persist方法的调用
* */
package com.cjs.sparkLearn.rddLearn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object CacheLearn{
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AggregateLearn")

    val sc = new SparkContext(conf)

    val rdd: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2, 3), List(4, 5, 6)), 2)

    val rdd1 = rdd.persist(StorageLevel.MEMORY_ONLY)
//    val rdd2 = rdd.cache()

  }
}

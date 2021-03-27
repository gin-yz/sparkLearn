package com.cjs.sparkLearn.rddLearn.OneVlaue

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object CacheLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AggregateLearn")

    val sc = new SparkContext(conf)

    val rdd: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2, 3), List(4, 5, 6)), 2)

    //两种方式存在内存中，是一样的．
    val rdd1 = rdd.persist(StorageLevel.MEMORY_ONLY)
    //    val rdd2 = rdd.cache()

    //还有几种级别的ｃａｃｈｅ存储,具体区别看教程
    //    rdd.persist(StorageLevel.DISK_ONLY)
    //    rdd.persist(StorageLevel.MEMORY_AND_DISK)

  }
}

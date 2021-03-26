package com.cjs.sparkLearn.rddLearn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CheckpointLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AggregateLearn")

    val sc = new SparkContext(conf)

//    sc.setCheckpointDir("checkpointTest") //设置检查点目录

    val rdd: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2, 3), List(4, 5, 6)), 2)

    val rdd2 = rdd.flatMap(x => x)

    val rdd3 = rdd2.map((_, 1))

    val rdd4 = rdd3.reduceByKey(_ + _)

//    rdd3.checkpoint()

    println(rdd4.getNumPartitions)

    println(rdd4.collect().toList)

    println(rdd4.toDebugString)



  }

}

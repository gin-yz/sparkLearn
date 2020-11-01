//按照传入的值进行分组，生成(key,Iterable[T])的形式
package com.cjs.sparkLearn.rddLearn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupByLearn {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupByLearn").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.makeRDD(1 to 16)

    val value1: RDD[(Int, Iterable[Int])] = value.groupBy(_ % 2)

    value1.foreach(println)


  }

}

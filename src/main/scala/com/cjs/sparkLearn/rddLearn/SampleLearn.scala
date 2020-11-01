/*
* 随机抽样
* 以指定的随机种子随机抽样出数量为 fraction 的数据，withReplacement 表示是抽
* 出的数据是否放回，true 为有放回的抽样，false 为无放回的抽样，seed 用于指定随机数生
* 成器种子。
* */

package com.cjs.sparkLearn.rddLearn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SampleLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupByLearn").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.makeRDD(1 to 16)

    val value1: RDD[Int] = value.sample(withReplacement = true, fraction = 0.5, seed = 428)

    println(value1.collect().toList)
  }

}

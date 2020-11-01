//将两个rdd，或者两个其他的列的项，一个一个的组合
//两个组合的ＲＤＤ的元素数量必须对应
package com.cjs.sparkLearn.rddLearn.twoValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ZipLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupByLearn").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val nameRdd: RDD[String] = sc.parallelize(Array("cjs", "cjs1", "cjs2"),3) //默认切片１６个
    val ageRdd :RDD[Int] = sc.parallelize(1 to 3,3)

    val personRdd: RDD[(String, Int)] = nameRdd.zip(ageRdd)

    println(personRdd.collect().toList)
  }

}

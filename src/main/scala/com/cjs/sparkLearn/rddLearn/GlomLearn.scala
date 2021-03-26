//glom将每个分区形成一个数组，类型为RDD[Array[T]]
package com.cjs.sparkLearn.rddLearn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GlomLearn {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GlomLearn")
    val sc = new SparkContext(conf)

    val value: RDD[Int] = sc.makeRDD(1 to 16,4)

    //每一个元素是一个数组
    val glomRDD: RDD[Array[Int]] = value.glom()

    glomRDD.collect().foreach(array=>{
      println(array.mkString(", "))
    })

    //计算每个分区的最大求和
    val value1:Double = glomRDD.map(_.foldLeft(Int.MinValue)(Math.max)).sum()

    println(value1)

    sc.stop()
  }
}

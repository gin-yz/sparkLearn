package com.cjs.sparkLearn.rddLearn.OneVlaue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FlatMapLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FlatMapLearn")

    val sc = new SparkContext(conf)
    //外层的Array会被剥去
    val value: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2, 3), List(4, 5, 6)))

    //不管是什么，转换成List就可以，之后flatMap会自动将List拆分
    val value1: RDD[Int] = value.flatMap(datas => datas)

    value1.foreach(println)

    val value2: RDD[Int] = sc.makeRDD(1 to 5)
    val value3: RDD[Int] = value2.flatMap(1 to _)
    println(value3.collect().toList)

    //目前只能想到这个方法，可能还有更妙的
    val value4: RDD[Any] = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))

    val value5 = value4.flatMap(x => x match {
      case x: Int => List[Int](x)
      case x: List[Int] => x
    })

    println(value5.collect().toList)

    sc.stop()

  }

}

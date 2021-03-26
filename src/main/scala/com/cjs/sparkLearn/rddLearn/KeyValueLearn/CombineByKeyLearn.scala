//可以将初始值进行包装
//求ｋｅｙ对应value的平均值
//aggregateByKey的进阶版，指定第一个值的数据结构
package com.cjs.sparkLearn.rddLearn.KeyValueLearn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CombineByKeyLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CombineByKeyLearn").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input: RDD[(String, Int)] = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)

    //(_,1)为第一个元素，如ｋｅｙ为ａ，假设第一个元素为("a",88),那么为(88,1),之后再进行分区内的操作．
    //等于说是用ｋｅｙ分组之后，对ｖａｌｕｅ操作的时候，把第一个ｖａｌｕｅ包装成(value,1)
    //第一个操作是在分区内，第二个是在分区间
    val value3: RDD[(String, (Int, Int))] = input.combineByKey((_, 1), (value: (Int, Int), v) =>
      (value._1 + v, value._2 + 1), (value1: (Int, Int), value2: (Int, Int)) => (value1._1 + value2._1, value1._2 + value2._2))

    value3.collect().foreach(item => {
      println(s"key:${item._1},value sum:${item._2._1},value count:${item._2._2}")
    })

    value3.foreach{case (key,(sum,count))=> println(s"$key,$sum,$count")}

    val value: RDD[Int] = value3.map { case (key, (sum, count)) => sum / count }

    println(value.collect().mkString("Array(", ", ", ")"))
  }

}

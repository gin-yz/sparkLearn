package com.cjs.sparkLearn.rddLearn.PartitionLearn

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapPartitionsLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("MapPartitionsLearn").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)

    //local模式下一个文件对应一个分区，
    val url: URL = MapPartitionsLearn.getClass.getResource("./mapPartitionsLearn")
    val value: RDD[String] = sc.textFile(url.toString)

    //打印每个分区
    value.foreachPartition(item => println(item.toList))

    println("************")

    //每个分区进行操作，将一个分区的数据发给一个exector
    //若用map,则数据要一个一个的发给excultor
    val value1: RDD[String] = value.mapPartitions(_.map(item => (item.toInt * 2).toString))

    value1.foreachPartition(item => println(item.toList))

    println("***********")

    //mapPartitionsWithIndex使用
    //将每一个分区中数据生成(分区号,数据)
    val indexValue: RDD[(Int, String)] = value.mapPartitionsWithIndex((index, items) => {
      items.map((index, _))
    })
    indexValue.foreach(println)

    //可以将小括号换成大括号
    val indexValue2: RDD[(String, Int)] = value.mapPartitionsWithIndex {
      (index, items) => {
        items.map((_, index))
      }
    }
    indexValue2.foreach(println)

    sc.stop()

  }
}

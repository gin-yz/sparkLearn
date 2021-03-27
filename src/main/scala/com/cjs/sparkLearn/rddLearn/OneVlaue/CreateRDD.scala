package com.cjs.sparkLearn.rddLearn.OneVlaue

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CreateRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("createRdd")
    val sc = new SparkContext(conf)

    //两种创建ＲＤＤ的方式，两种方式没有区别,底层都是parallelize
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 20) //后接并行数，对应的分区，最后结果也是分区，有多少最后生成多少文件
    val arrayRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5, 6))

    //使用collect方法又转回来了，默认转换的是Ａrray
    val newList: List[Int] = listRDD.collect().toList
    val newArray: Array[Int] = arrayRDD.collect()

    newList.foreach(println)
    newArray.foreach(println)

    //从外部读生成ＲＤＤ
    val url: URL = CreateRDD.getClass.getResource("./createDir")
    //默认读文件是一行一行读
    val localRDD: RDD[String] = sc.textFile(url.toString, 2) //后接最小并行数，对应的分区，最后结果也是分区，有多少最后生成多少文件(但也不一定，有可能指定２出来３个文件)
    val tuples: Array[(String, Int)] = localRDD.flatMap(item => item.split(" ")).map((_, 1)).groupByKey().map(item => (item._1, item._2.sum)).collect()
    val tuples1: Array[(String, Int)] = localRDD.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect()
    tuples.foreach(println)
    tuples1.foreach(println)

    //结果保存
    //    localRDD.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).saveAsTextFile("output")

    //    val url2: URL = CreateRDD.getClass.getResource("./createRDD3.txt")
    //    sc.textFile(url2.toString,2).saveAsTextFile("output")

  }
}

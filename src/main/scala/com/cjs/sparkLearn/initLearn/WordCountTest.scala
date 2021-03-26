package com.cjs.sparkLearn.initLearn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object WordCountTest {
  def main(args: Array[String]): Unit = {
    //在集群上跑的时候setMaster("local[*]")不设置
    val conf: SparkConf = new SparkConf().setAppName("wordcountdemo") //local[*]

    val sc = new SparkContext(conf)

//    println(WordCount.getClass.getResource("./createRDD.txt").toString)

    //装饰器模式层层包装
    val value: RDD[String] = sc.textFile("hdfs://hadoop1:8020/tmp/sparklearn/wordcount.txt");
//    val value: RDD[String] = sc.textFile(WordCount.getClass.getResource("./createRDD.txt").toString)

    val value1: RDD[String] = value.flatMap(_.split(" "))

    val value2: RDD[(String, Int)] = value1.map((_, 1))

    val value3: RDD[(String, Int)] = value2.reduceByKey(_ + _) //reduceByKey将相同key的聚合,对应的操作是在一个ｋｅｙ下

    val tuples: Array[(String, Int)] = value3.collect()

    sc.stop()
    println(tuples.mkString("Array(", ", ", ")"))

  }
}

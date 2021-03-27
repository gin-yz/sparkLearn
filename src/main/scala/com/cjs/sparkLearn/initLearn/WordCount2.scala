package com.cjs.sparkLearn.initLearn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object WordCount2 {
  def main(args: Array[String]): Unit = {
    //在集群上跑的时候setMaster("local[*]")不设置
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcountdemo") //local[*]

    val sc = new SparkContext(conf)

    println(WordCount.getClass.getResource("./createRDD.txt").toString)

    //装饰器模式层层包装
//    val value: RDD[String] = sc.textFile("hdfs://hadoop1:8020/tmp/sparklearn/wordcount.txt");
    //默认是按照行来取，一个行分配成一
    val value: RDD[String] = sc.textFile(WordCount.getClass.getResource("./createRDD.txt").toString)
    println(value.dependencies)

    val value1: RDD[String] = value.flatMap(_.split(" "))
    println(value1.dependencies)

    val value2: RDD[(String, Int)] = value1.map((_, 1))
    println(value2.dependencies)

    val value3: RDD[(String, Int)] = value2.reduceByKey(_ + _) //reduceByKey将相同key的聚合,对应的操作是在一个ｋｅｙ下
    println(value3.dependencies)

    val tuples: Array[(String, Int)] = value3.collect()

    //方法二，使用个group by
    val value4:RDD[(String,Int)] = value.flatMap(_.split(" ")).groupBy(x=>x).map(item =>
      (item._1, item._2.foldLeft(0)((sum, x) => sum + 1))
    )
    val tuples2 = value4.collect().toList
    //或者
    val value5 = value.flatMap(_.split(" ")).groupBy(x => x).map(item=>(item._1,item._2.count(_=>true))).collect().toList


    sc.stop()
    println(tuples.mkString("Array(", ", ", ")"))
    println(tuples2)
    println(value5)
  }
}

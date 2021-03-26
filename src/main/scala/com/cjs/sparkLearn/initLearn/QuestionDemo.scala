package com.cjs.sparkLearn.initLearn

import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object QuestionDemo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcountdemo") //local[*]

    val sc = new SparkContext(conf)

    val value = sc.textFile(QuestionDemo.getClass.getResource("./createRDD.txt").getFile)

    val value1:RDD[Array[String]] = value.map(_.split(" "))

    //问题!!!!!!!!如果我非要这样得到一个RDD[Array[String]]，接下来怎样转换到RDD[String]?

    //解法１
    val value2 = value1.flatMap(x => x)
    println(value2.collect().toList)

    //解法２


    sc.stop()

  }

}

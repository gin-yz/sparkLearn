//只对(k,v)中的v进行操作
package com.cjs.sparkLearn.rddLearn.KeyValueLearn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object MapValuesLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //Array(Array((a,3), (a,2), (c,4)), Array((b,3), (c,6), (c,8)))
    val rdd: RDD[(String, Int)] = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)

    val value: RDD[(String, Int)] = rdd.mapValues(_ + 10)

    println(value.collect().toList)
  }
}

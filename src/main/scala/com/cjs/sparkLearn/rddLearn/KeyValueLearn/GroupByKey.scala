//groupByKey只是粗暴的把相同ｋｅｙ的放在一起，也可以指定分区数，是在运行时体现的．partitiionBy是存储的时候放在不同的地方，在运行时还是在一个序列中
package com.cjs.sparkLearn.rddLearn.KeyValueLearn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GroupByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupByKey").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd1: RDD[(Int, String)] = sc.parallelize(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (4, "ddd"),(4, "eee")), 4)

    val value: RDD[(Int, Iterable[String])] = rdd1.groupByKey(1)

    println(value.collect().toList)//List((3,CompactBuffer(ccc)), (4,CompactBuffer(ddd, eee)), (1,CompactBuffer(aaa)), (2,CompactBuffer(bbb)))

//    value.saveAsTextFile("output") //三个文件，
  }

}

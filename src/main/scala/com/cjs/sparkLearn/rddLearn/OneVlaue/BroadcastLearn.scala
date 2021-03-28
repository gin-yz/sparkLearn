//当有共享常量或者使用到小表join大表的时候，可以使用广播
package com.cjs.sparkLearn.rddLearn.OneVlaue

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BroadcastLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("BroadcastLearn")

    val sc = new SparkContext(conf)

    //ｊｏｉｎ操作，因为是迪卡尔积，所以会有ｓｕｆｆｌｅ，分区会重新排，托慢速度
    val value = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("d", 4)), 4)
    val value2 = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6), ("d", 7)), 4)
    val joinRdd: RDD[(String, (Int, Int))] = value.join(value2)
    println(joinRdd.collect().toList)

    //解决方法，使用Broadcast，将小表申明成Broadcast
    val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("d", 4)), 4)
    val map = Map(("a", 4), ("b", 5), ("c", 6), ("d", 7))
    // 声明广播变量
    val broadcast: Broadcast[Map[String, Int]] = sc.broadcast(map)
    val filterRdd:RDD[(String,Int)]= rdd1.filter (
      item => broadcast.value.contains(item._1)
    )
    val resultRDD = filterRdd.map { item =>
      (item._1, item._2, broadcast.value.getOrElse(item._1,None))
    }

    resultRDD.foreach(println)

    val rdd2 = sc.makeRDD(List( ("a",1), ("b", 2), ("c", 3), ("d", 4) ),4)
    val list2 = List( ("a",4), ("b", 5), ("c", 6), ("d", 7) )
    // 声明广播变量
    val broadcast2: Broadcast[List[(String, Int)]] = sc.broadcast(list)
    val resultRDD2: RDD[(String, (Int, Int))] = rdd2.map {
      case (key, num) => {
        var num2 = 0
        // 使用广播变量
        for ((k, v) <- broadcast2.value) {
          if (k == key) {
            num2 = v
          }
        }
        (key, (num, num2))
      }
    }
    resultRDD2.foreach(println)

    sc.stop()
  }

}

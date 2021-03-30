//第一版的统计，有很多性能的问题，下面的会逐步的改善
package com.cjs.sparkLearn.rddLearn.ExampleLearn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Required1 {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local[*]").setAppName("Required1")
    val sc = new SparkContext(sparConf)
    val actionRDD = sc.textFile(Test.getClass.getResource("./user_visit_action.txt").getFile)

    actionRDD.cache()

    //统计品类的点击数量：（品类ID，点击数量）
    val clickCountRDD = actionRDD
      .map(_.split("_")) //分割一行数据
      .filter(arr => arr(6) != "-1") //将品类值为－１的去掉
      .map(arr => (arr(6), 1)) //映射成(String,1)
      .reduceByKey(_+_)

    //统计品类的下单数量：（品类ID，下单数量）,下单数量一行有多个，使用＂，＂隔开
    val orderCountRDD = actionRDD
      .map(_.split("_"))
      .filter(arr => arr(8) != "null")
      .flatMap(arr => arr(8).split(","))
      .map((_, 1))
      .reduceByKey(_+_)

    //统计品类的支付数量：（品类ID，支付数量）
    val payCountRDD = actionRDD
      .map(_.split("_"))
      .filter(arr => arr(10) != "null")
      .flatMap(arr => arr(10).split(","))
      .map((_, 1))
      .reduceByKey(_+_)

    //先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
    //将三个ｒｄｄ合并，cogroup存在shuffle现象,性能会下降
    val cogroupRDDRaw: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountRDD.cogroup(orderCountRDD, payCountRDD)

    val cogroupRDD = cogroupRDDRaw.mapValues {
      case (clickIter, orderIter, payIter) => {
        var clickCnt = 0
        val iter1 = clickIter.iterator
        if (iter1.hasNext) {
          clickCnt = iter1.next()
        }
        var orderCnt = 0
        val iter2 = orderIter.iterator
        if (iter2.hasNext) {
          orderCnt = iter2.next()
        }
        var payCnt = 0
        val iter3 = payIter.iterator
        if (iter3.hasNext) {
          payCnt = iter3.next()
        }
        (clickCnt, orderCnt, payCnt)
      }
    }

    val resultRDD = cogroupRDD.sortBy(_._2, ascending = false)

    println(resultRDD.collect().toList)

    //之前写废的
    //按照三个总和排名
    //    val sumCountList = clickCountMap
    //      .map(item_one => {
    //        val orderCount = orderCountMap.getOrElse(item_one._1, 0L)
    //        val payCount = payCountMap.getOrElse(item_one._1, 0L)
    //        (item_one._1, item_one._2 + orderCount + payCount)
    //      })
    //      .toList
    //      .sortBy(item => item._2)

    //先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
    //    val sumCountList2 = clickCountMap
    //      .toList
    //      .sortWith { (item1, item2) =>
    //        if (item1._2 > item2._2) true
    //        else if (item1._2 < item2._2) false
    //        else {
    //          val item1OrderCount = orderCountMap.getOrElse(item1._1, 0L)
    //          val item2OrderCount = orderCountMap.getOrElse(item1._1, 0L)
    //          if (item1OrderCount > item2OrderCount) true
    //          else if (item1OrderCount < item2OrderCount) false
    //          else {
    //            val item1PayCount = payCountMap.getOrElse(item1._1, 0L)
    //            val item2PayCount = payCountMap.getOrElse(item1._1, 0L)
    //            if (item1PayCount > item2PayCount) true
    //            else false
    //          }
    //        }
    //      }
    //      .map(_._1)

  }
}

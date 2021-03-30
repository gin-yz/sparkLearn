//改进将三个ｒｄｄ合并的过程中，cogroup存在shuffle现象,性能会下降
package com.cjs.sparkLearn.rddLearn.ExampleLearn

import org.apache.spark.{SparkConf, SparkContext}

object Required1_adv {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local[*]").setAppName("Required1")
    val sc = new SparkContext(sparConf)
    val actionRDD = sc.textFile(Test.getClass.getResource("./user_visit_action.txt").getFile)

    actionRDD.cache()

    //统计品类的点击数量：（品类ID，点击数量）
    val clickCountRDD = actionRDD
      .map(_.split("_"))
      .filter(arr => arr(6) != "-1")
      .map(arr => (arr(6), 1))
      .reduceByKey(_ + _)

    //统计品类的下单数量：（品类ID，下单数量）,下单数量一行有多个，使用＂，＂隔开
    val orderCountRDD = actionRDD
      .map(_.split("_"))
      .filter(arr => arr(8) != "null")
      .flatMap(arr => arr(8).split(","))
      .map((_, 1))
      .reduceByKey(_ + _)

    //统计品类的支付数量：（品类ID，支付数量）
    val payCountRDD = actionRDD
      .map(_.split("_"))
      .filter(arr => arr(10) != "null")
      .flatMap(arr => arr(10).split(","))
      .map((_, 1))
      .reduceByKey(_ + _)

    val clickCountRDD_adv = clickCountRDD
      .map {
        case (key, value) => {
          (key, (value, 0, 0))
        }
      }

    val orderCountRDD_adv = orderCountRDD
      .map {
        case (key, value) => {
          (key, (0, value, 0))
        }
      }

    val payCountRDD_adv = payCountRDD
      .map {
        case (key, value) => {
          (key, (0, 0, value))
        }
      }

    val cogroupRDD = clickCountRDD_adv
      .union(orderCountRDD_adv)
      .union(payCountRDD_adv)
      .reduceByKey {
        case (item1, item2) =>
          (item1._1 + item2._1, item1._2 + item2._2, item1._3 + item2._3)
      }

    val resultRDD = cogroupRDD.sortBy(_._2, ascending = false)

    println(resultRDD.collect().toList)

  }
}

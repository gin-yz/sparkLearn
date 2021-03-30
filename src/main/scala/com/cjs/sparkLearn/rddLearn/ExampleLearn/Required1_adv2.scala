//继续改进，不用创建三个ｒｄｄ再聚合了，直接放到一个方法里
package com.cjs.sparkLearn.rddLearn.ExampleLearn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Required1_adv2 {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local[*]").setAppName("Required1")
    val sc = new SparkContext(sparConf)
    val actionRDD = sc.textFile(Required1_adv2.getClass.getResource("./user_visit_action.txt").getFile)

    actionRDD.cache()
    // 2. 将数据转换结构
    //    点击的场合 : ( 品类ID，( 1, 0, 0 ) )
    //    下单的场合 : ( 品类ID，( 0, 1, 0 ) )
    //    支付的场合 : ( 品类ID，( 0, 0, 1 ) )
    val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          // 点击的场合,因为只有一个，但是调用的是flapMap的方法，所以需要List包装
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          // 下单的场合
          val ids = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {
          // 支付的场合
          val ids = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    // 3. 将相同的品类ID的数据进行分组聚合
    //    ( 品类ID，( 点击数量, 下单数量, 支付数量 ) )
    val analysisRDD: RDD[(String, (Int, Int, Int))] = flatRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    // 4. 将统计结果根据数量进行降序处理，取前10名
    val resultRDD = analysisRDD.sortBy(_._2, ascending = false)

    // 5. 将结果采集到控制台打印出来
    resultRDD.foreach(println)

    sc.stop()

  }

}

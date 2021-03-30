package com.cjs.sparkLearn.rddLearn.ExampleLearn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Required3 {

  case class UserVisitAction(
                              date: String, //用户点击行为的日期
                              user_id: Long, //用户的ID
                              session_id: String, //Session的ID
                              page_id: Long, //某个页面的ID
                              action_time: String, //动作的时间点
                              search_keyword: String, //用户搜索的关键词
                              click_category_id: Long, //某一个商品品类的ID
                              click_product_id: Long, //某一个商品的ID
                              order_category_ids: String, //一次订单中所有品类的ID集合
                              order_product_ids: String, //一次订单中所有商品的ID集合
                              pay_category_ids: String, //一次支付中所有品类的ID集合
                              pay_product_ids: String, //一次支付中所有商品的ID集合
                              city_id: Long //城市 id
                            )

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparConf)

    val actionRDD = sc.textFile(Spark06_Req3_PageflowAnalysis.getClass.getResource("./user_visit_action.txt").getFile)

    val actionDataRDD = actionRDD.map(
      action => {
        val datas = action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    )
    actionDataRDD.cache()

    val listIndex: List[Long] = List[Long](1, 2, 3, 4, 5, 6, 7)
    val valueRDD: RDD[UserVisitAction] = actionDataRDD
      .filter(item => listIndex
        .contains(item.page_id))

    valueRDD.cache()

    val sumRDD: RDD[(Long, Int)] = valueRDD
      .map(item=>
        (item.page_id,1))
      .reduceByKey(_+_)

    //scala中实现的有滑窗(sliding)和zip将几个数进行组合
    val resultMap: collection.Map[Long, Long] = valueRDD
      .groupBy(_.session_id)
      .map {
        case (sessionKey, iter) => {
          iter.toList.sortBy(_.action_time).map(_.page_id)
        }
      }
      .flatMap { item =>
        item
          .zip(item.slice(1, item.length))
          .filter(item => item._1 + 1 == item._2)
          .map(_._1)
      }
      .countByValue()

    val sumMap: Map[Long, Int] = sumRDD.collect().toMap
    println(sumMap)
    println(resultMap)

    sumMap.foreach {
      case (key, value) => {
        println(s"页面${key}跳转到页面${key + 1}单跳转换率为:" + (resultMap.getOrElse(key, 0L).toDouble / value.toDouble))
      }
    }

    sc.stop()


  }


}

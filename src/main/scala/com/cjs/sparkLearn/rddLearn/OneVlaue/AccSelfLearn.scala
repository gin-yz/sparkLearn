//自定义实现累加器
package com.cjs.sparkLearn.rddLearn.OneVlaue

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object AccSelfLearn {
  def main(args: Array[String]): Unit = {
    //前面写的wordcount都存在ｓｈｕｆｆｌｅ操作，不是很高效，如果我们一开始就搞一个累加器，然后整个ｆｏｒｅａｃｈ遍历一下，就没有ｓｈｕｆｆｌｅ的操作，很高效
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SelfAccLearn")

    val sc = new SparkContext(conf)

    val value: RDD[String] = sc.makeRDD(List("cjs", "cjs1", "cjs1", "cjs", "dsg", "dsg", "dsg2"))

    // 累加器 : WordCount
    // 创建累加器对象
    val wcAcc = new MyAccumulator()
    // 向Spark进行注册
    sc.register(wcAcc, "wordCountAcc")

    value.foreach(x =>
      wcAcc.add(x)
    )

    println(wcAcc.value.toList)

  }

  /*
     自定义数据累加器：WordCount
     1. 继承AccumulatorV2, 定义泛型
        IN : 累加器输入的数据类型 String
        OUT : 累加器返回的数据类型 mutable.Map[String, Long]
     2. 重写方法（6）
    */
  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

    private var wcMap = mutable.Map[String, Long]()

    // 判断是否为初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    // 复制累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    // 重置累加器
    override def reset(): Unit = {
      wcMap.clear()
    }

    // 获取累加器需要计算的值
    override def add(word: String): Unit = {
      val newCnt = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word, newCnt)
    }

    // Driver合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {

      val map1 = this.wcMap
      val map2 = other.value

      map2.foreach {
        case (key,value)=>
          val newValue = map1.getOrElse(key, 0L) + value
          map1.update(key, newValue)
      }
    }

    // 累加器结果
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }
}

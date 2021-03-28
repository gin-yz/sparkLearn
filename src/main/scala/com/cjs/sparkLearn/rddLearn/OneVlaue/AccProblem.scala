//acc采集器，又叫累加器，主要的功能是使得在Executor执行的变量可以返回到ｄｒｉｖｅｒ端
package com.cjs.sparkLearn.rddLearn.OneVlaue

import org.apache.spark.{SparkConf, SparkContext}

object AccProblem {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AccProblem")

    val sc = new SparkContext(conf)

    val value = sc.makeRDD(List(1, 2, 3, 4, 5))

    var sum: Int = 0

    value.foreach(x =>
      sum = sum + x
    )
    //ｓｕｍ为０，因为在Executor执行的时候ｓｕｍ是发给每个ｅｘｃｕｔｏｒ的，但是执行完后，Executor并没有将ｓｕｍ返还回来
    //而且如果是多个ｅｘｃｕｔｏｒ一起执行，还涉及到同步的操作
    println(sum)

    //解决办法，使用ａｃｃ累加器,自动实现了同步的操作，相当于是个临界区
    val sumAcc = sc.longAccumulator("sum")
    value.foreach(x=>
      sumAcc.add(x)
    )
    //注意一下，如果ｍａｐ是的话，因为是转换算子，所以并不会触发，结果仍为０．使用ｃｏｌｌｅｃｔ()触发
//    value.map(x=>
//      sumAcc.add(x)
//    )
    println(sumAcc.value)
    //其他类型的累加器
//    sc.collectionAccumulator("sum") //累加对象
//    sc.doubleAccumulator("sum")
  }

}

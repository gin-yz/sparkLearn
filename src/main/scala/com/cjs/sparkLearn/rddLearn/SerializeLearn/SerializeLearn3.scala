package com.cjs.sparkLearn.rddLearn.SerializeLearn

import org.apache.spark.{SparkConf, SparkContext}

object SerializeLearn3 {
  def main(args: Array[String]): Unit = {
    //1.创建 SparkConf 并设置 App 名称
    val conf: SparkConf = new SparkConf().setAppName("SerialLearnTest").setMaster("local[*]")
    //2.创建 SparkContext，该对象是提交 Spark App 的入口
    val sc: SparkContext = new SparkContext(conf)
    //3.创建一个 RDD
    val rdd = sc.makeRDD(List[Int]())

    val user = new User()

    //spark自动进行了闭包的检查，现在ｒｄｄ里面为空，ｆｏｒｅａｃｈ应该没有执行，应该不会报错，但是ｓｐａｒｋ进行了闭包检查，Ｕｓｅｒ没有序列化，所以抛出异常．
    rdd.foreach(num => {
      println(user.age + num)
    })

  }

  class User {
    val age = 10;
  }

}

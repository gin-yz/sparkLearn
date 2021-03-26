package com.cjs.sparkLearn.rddLearn.SerializableLearn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SerialLearn {
  def main(args: Array[String]): Unit = {
    //1.创建 SparkConf 并设置 App 名称
    val conf: SparkConf = new SparkConf().setAppName("SerialLearnTest").setMaster("local[*]")
    //2.创建 SparkContext，该对象是提交 Spark App 的入口
    val sc: SparkContext = new SparkContext(conf)
    //3.创建一个 RDD
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark",
      "hive", "atguigu"))
    //3.1 创建一个 Search 对象
    val search = new Search("hello")
    //3.2 函数传递，打印：ERROR Task not serializable
    search.getMatch1(rdd).collect().foreach(println)
    //3.3 属性传递，打印：ERROR Task not serializable
    search.getMatch2(rdd).collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }

  //继承Serializable类后，会自动的序列化,若不加的话，调用getMatch1方法，isMatch方法会在Ｄｒｉｖｅｒ端，而不是在ｅｘｅｃｕｔｏｒ端，无法运行．
  class Search(query: String) extends Serializable {
    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    // 函数序列化案例
    def getMatch1(rdd: RDD[String]): RDD[String] = {
      //rdd.filter(this.isMatch)
      rdd.filter(isMatch)
    }

    //重要！需要序列化，因为需要传自己赋值的ｑｕｅｒｙ去ｅｘｅｃｕｔｏｒ，自己赋值的query虽然是Ｓｔｒｉｎｇ类型，但是机器将其读成了一个对象的属性，而这个对象没有序列化，所以会报错
    //若将ｑｕｅｒｙ重新赋值给另外一个ｓｔｒｉｎｇ，那么ｓｐａｒｋ就不会将其解读成对象的属性，而是解读成ｓｔｒｉｎｇ，因为ｓｔｒｉｎｇ本来就是序列化了，所以不会出错
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      //这样就不需要序列化了
//      val q= query
//      rdd.filter(x=>x.contains(q))
      rdd.filter(x => x.contains(query))
      //val q = query
      //rdd.filter(x => x.contains(q))
    }

    //此方法不用进行序列化,因为返回的就是一个ｔｒｕｅ，而ｔｒｕｅ已经自动序列化了
    def getMatch3(rdd: RDD[String]): RDD[String] = {
      //rdd.filter(x => x.contains(this.query))
      rdd.filter(_=>true)
      //val q = query
      //rdd.filter(x => x.contains(q))
    }

  }

}

//新方法实现ＵＤＡＦ
//适用于spark3.0及以上
package com.cjs.sparkLearn.sqlLearn

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

object SqlUDAFLearn2 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val builder: SparkSession.Builder = SparkSession.builder()
    val spark: SparkSession = builder.config(sparkConf).getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.read.json(SqlUDAFLearn.getClass.getResource("./user.json").getFile)

    df.createOrReplaceTempView("user")

//    spark.udf.register("ageAvg", functions.udaf(new MyAvgUDAF()))

//    spark.sql("select ageAvg(age) from user").show


    // TODO 关闭环境
    spark.close()
  }

  /*
 自定义聚合函数类：计算年龄的平均值
 1. 继承org.apache.spark.sql.expressions.Aggregator, 定义泛型
     IN : 输入的数据类型 Long
     BUF : 缓冲区的数据类型 Buff
     OUT : 输出的数据类型 Long
 2. 重写方法(6)
 */
  case class Buff( var total:Long, var count:Long )
  class MyAvgUDAF extends Aggregator[Long, Buff, Long]{
    // z & zero : 初始值或零值
    // 缓冲区的初始化
    override def zero: Buff = {
      Buff(0L,0L)
    }

    // 根据输入的数据更新缓冲区的数据
    override def reduce(buff: Buff, in: Long): Buff = {
      buff.total = buff.total + in
      buff.count = buff.count + 1
      buff
    }

    // 合并缓冲区
    override def merge(buff1: Buff, buff2: Buff): Buff = {
      buff1.total = buff1.total + buff2.total
      buff1.count = buff1.count + buff2.count
      buff1
    }

    //计算结果
    override def finish(buff: Buff): Long = {
      buff.total / buff.count
    }

    // 缓冲区的编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    // 输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}

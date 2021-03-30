package com.cjs.sparkLearn.sqlLearn

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveLearn {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    //注意enableHiveSupport
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    // 使用SparkSQL连接外置的Hive
    // 1. 拷贝Hive-size.xml文件到classpath下
    // 2. 启用Hive的支持
    // 3. 增加对应的依赖关系（包含MySQL驱动）
    //查看ｔｅｓｔｄｂ库中的表
    spark.sql("use testdb")
    spark.sql("show tables").show

    spark.close()
  }

}

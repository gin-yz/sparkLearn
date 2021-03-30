package com.cjs.sparkLearn.sqlLearn

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object JDBCLearn {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    // 读取MySQL数据
    val df: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://192.168.126.132:11111/sakila?characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "cjsdsg")
      .option("dbtable", "city")
      .load()

    //方式 2:使用 jdbc 方法读取
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "cjsdsg")
    val df1: DataFrame = spark.read.jdbc("jdbc:mysql://192.168.126.132:11111/sakila?characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false",
      "city", props)
    df1.show


    df1.show()

    //保存数据
    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://192.168.126.132:11111/SpaekSql?characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "cjsdsg")
      .option("dbtable", "Test")
      .mode(SaveMode.Append)
      .save()

    //方式 2：通过 jdbc 方法
    val props1: Properties = new Properties()
    props1.setProperty("user", "root")
    props1.setProperty("password", "123123")
    df1.write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.126.132:11111/SpaekSql?characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false",
      "Test", props)

  }

}

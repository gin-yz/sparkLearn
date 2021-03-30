package com.cjs.sparkLearn.sqlLearn

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object JoinLearn2 {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val builder: SparkSession.Builder = SparkSession.builder()
    val spark: SparkSession = builder.config(sparkConf).getOrCreate()
    //如果需要 RDD 与 DF 或者 DS 之间互相操作，那么需要引入.在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
    import spark.implicits._

    val df1: DataFrame = spark.createDataset(Seq(("a", 1,2), ("b",2,3) )).toDF("k1","k2","k3")
    val df2: DataFrame = spark.createDataset(Seq(("a", 2,2), ("b",3,3), ("b", 2,1), ("c", 1,1)) ).toDF("k1","k2","k4")

    df1.createOrReplaceTempView("test1")
    df2.createOrReplaceTempView("test2")

    val df3: DataFrame = spark.sql("select T1.k1 as k1_1,T2.k1 as k1_2 from test1 as T1 left join test2 as T2 on T1.k1 = T2.k1")


    spark.close()
  }

}

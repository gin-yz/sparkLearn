package com.cjs.sparkLearn.sqlLearn

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object JoinLearn {

  case class User(age: Int, name: String, info: String)

  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val builder: SparkSession.Builder = SparkSession.builder()
    val spark: SparkSession = builder.config(sparkConf).getOrCreate()
    //如果需要 RDD 与 DF 或者 DS 之间互相操作，那么需要引入.在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
    import spark.implicits._

    val df: DataFrame = spark.read.json(DfAndDsLearn.getClass.getResource("./user.json").getFile)

    //将name和age做相应操作后改名
    val df2: DataFrame = df.select($"name" as "nameAli", $"age" + 2020 as "ageAli", $"info")
    df2.show()

    /**
     * join比较通用两种调用方式，注意在usingColumns里的字段必须在两个DF中都存在
     * joinType：默认是 `inner`. 必须是以下类型的一种:
     * `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`,`right`, `right_outer`, `left_semi`, `left_anti`.
     */
    println(df.apply("name") === df2.apply("nameAli"))
    val df3: DataFrame = df.join(df2, df.apply("name") === df2.apply("nameAli"), "inner")
    //此时字段中出现了两个info的列
    df3.show()
    //重命名列可以解决重复的问题
    df.withColumnRenamed("info","info_1").join(df2, df.apply("name") === df2.apply("nameAli"), "inner").show()

    val df4: DataFrame = spark.createDataset(Seq(("a", 1,2), ("b",2,3) )).toDF("k1","k2","k3")
    val df5: DataFrame = spark.createDataset(Seq(("a", 2,2), ("b",3,3), ("b", 2,1), ("c", 1,1)) ).toDF("k1","k2","k4")

    //多个字段比较
    df4.join(df5,Seq("k1","k2")).show()

    spark.close()
  }

}

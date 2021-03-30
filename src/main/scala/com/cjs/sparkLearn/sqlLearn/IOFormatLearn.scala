package com.cjs.sparkLearn.sqlLearn

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object IOFormatLearn {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val builder: SparkSession.Builder = SparkSession.builder()
    val spark: SparkSession = builder.config(sparkConf).getOrCreate()
    import spark.implicits._
    /**
     * Scala/Java Any Language Meaning
     * SaveMode.ErrorIfExists(default) "error"(default) 如果文件已经存在则抛出异常
     * SaveMode.Append "append" 如果文件已经存在则追加
     * SaveMode.Overwrite "overwrite" 如果文件已经存在则覆盖
     * SaveMode.Ignore "ignore" 如果文件已经存在则忽略
     */
    val df: DataFrame = spark.read.json(IOFormatLearn.getClass.getResource("./user.json").getFile)

    df.write.mode("append").json("output")

    //CSV文件的读取
    val df2: DataFrame = spark.read.format("csv")
      .option("sep", ";").option("inferSchema",
      "true")
      .option("header", "true")
      .load("data/user.csv")


    spark.close()
  }

}

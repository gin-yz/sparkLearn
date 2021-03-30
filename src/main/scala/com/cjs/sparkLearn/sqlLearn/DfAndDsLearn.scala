package com.cjs.sparkLearn.sqlLearn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DfAndDsLearn {

  case class User(age: Int, name: String, info: String)

  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val builder: SparkSession.Builder = SparkSession.builder()
    val spark: SparkSession = builder.config(sparkConf).getOrCreate()
    //可以通过SparkSession得到SparkContext
//    val sc: SparkContext = spark.sparkContext
    //如果需要 RDD 与 DF 或者 DS 之间互相操作，那么需要引入.在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
    import spark.implicits._

    val df: DataFrame = spark.read.json(DfAndDsLearn.getClass.getResource("./user.json").getFile)
    //    spark.read.format("json")

    // DataFrame => SQL
    df.createOrReplaceTempView("user")
    val df2: DataFrame = spark.sql("select * from user")
    df2.show()
    println(df2.rdd.collect().toList)

    spark.sql("select age,name from user").show
    spark.sql("select avg(age) from user").show

    // DataFrame => DSL
    // 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则,import spark.implicits._
    df.select("age", "name").show
    df.select($"age" + 1 as "age plus 1").show //别名
    df.select('age + 1).show

    // TODO DataSet
    // DataFrame其实是特定泛型的DataSet
    val seq: Seq[Int] = Seq(1, 2, 3, 4)
    val ds: Dataset[Int] = seq.toDS()
    ds.show()

    // RDD <=> DataFrame
    val rdd1: RDD[(Int, String, String)] = spark.sparkContext.makeRDD(List((22, "zhangsan", "hehe"), (23, "lisi", "dsg")))
    val df1: DataFrame = rdd1.toDF("age", "name", "info")
    df1.show()
    //df=>rdd
    val rowRDD: RDD[Row] = df1.rdd
    val toList: List[(Int, String, String)] = rowRDD
      .map {
        row => {
          (row.apply(0).asInstanceOf[Int], row.apply(1).asInstanceOf[String], row.apply(2).asInstanceOf[String])
        }
      }.collect()
      .toList
    println(toList)

    // DataFrame <=> DataSet
    val ds3: Dataset[User] = df.as[User]
    val df3: DataFrame = ds3.toDF()

    // RDD <=> DataSet
    val ds4: Dataset[User] = rdd1.map {
      case (age, name, info) => {
        User(age, name, info)
      }
    }.toDS()
    val userRDD: RDD[User] = ds4.rdd


    // TODO 关闭环境

    spark.close()
  }

}

//将一个列中的属性，比如是ｌｉｓｔ，展开成多行
package com.cjs.sparkLearn.sqlLearn

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable

object SqlUDTFLearn {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val builder: SparkSession.Builder = SparkSession.builder()
    val spark: SparkSession = builder.config(sparkConf).getOrCreate()
    import spark.implicits._

    //将json中的某一列若是ｌｉｓｔ读取
    val df: DataFrame = spark.read.json(SqlUDTFLearn.getClass.getResource("./user2.json").getFile)

    val rdd1: RDD[Row] = df.select("info").rdd

    val rdd2: RDD[String] = rdd1.flatMap {
      row => {
        row.apply(0).asInstanceOf[mutable.WrappedArray[String]]
      }
    }

    println(rdd2.collect().toList)

    //将每一列的list展开
    val ds: Dataset[String] = df
      .select("info")
      .flatMap {
        row => {
          row.apply(0).asInstanceOf[mutable.WrappedArray[String]]
        }
      }
    ds.show()

    //自定义udtf,展开
    val df2: DataFrame = df
      .select("name", "age", "info")
      .flatMap {
        row => {
          row
            .apply(2)
            .asInstanceOf[mutable.WrappedArray[String]]
            .toList
            .map {
              x =>
                (row.apply(0).asInstanceOf[String], row.apply(1).asInstanceOf[Long], x)
            }
        }
      }
      .toDF("name", "age", "info")
    df2.show()

    spark.close()
  }

}

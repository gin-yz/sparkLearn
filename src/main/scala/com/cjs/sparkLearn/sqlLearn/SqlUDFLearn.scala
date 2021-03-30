//udf=>将某个列中的每个值做相应变换
package com.cjs.sparkLearn.sqlLearn

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SqlUDFLearn {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val builder: SparkSession.Builder = SparkSession.builder()
    val spark: SparkSession = builder.config(sparkConf).getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.read.json(DfAndDsLearn.getClass.getResource("./user.json").getFile)

    spark.udf.register("prefixName", (name: String) => {
      name + "," + name
    })

    df.createOrReplaceTempView("User")
    val df2: DataFrame = spark.sql("select prefixName(name) from User")

    df2.show()

    spark.close()

  }

}

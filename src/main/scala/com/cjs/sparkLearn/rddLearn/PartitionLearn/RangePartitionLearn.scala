//按照ｋｅｙ的取值范围来分区
package com.cjs.sparkLearn.rddLearn.PartitionLearn

import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

object RangePartitionLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("RangePartitionLearn").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val value = sc.makeRDD(List(1 to 100:_*))

    val value1 = value.map((_, 1))

    //分成五段，１－２０，２１－４０......
    val value2 = value1.partitionBy(new RangePartitioner(5, value1))

    println(value2.glom().map(_.toList).collect().toList)

  }

}

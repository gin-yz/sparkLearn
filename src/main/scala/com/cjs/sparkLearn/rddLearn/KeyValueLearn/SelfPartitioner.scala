//只有严格遵守key:value的算子，才有partitionBy方法

package com.cjs.sparkLearn.rddLearn.KeyValueLearn

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SelfPartitioner {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SelfPartitioner").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd1: RDD[(Int, String)] = sc.parallelize(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (4, "ddd")))

    val value: RDD[(Int, String)] = rdd1.partitionBy(new MyPartitionar(3))

    println(value.glom().map(_.toList).collect().toList)

  }
}

class MyPartitionar(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions //分区数

  override def getPartition(key: Any): Int = {
    key.hashCode() % numPartitions
  }
}

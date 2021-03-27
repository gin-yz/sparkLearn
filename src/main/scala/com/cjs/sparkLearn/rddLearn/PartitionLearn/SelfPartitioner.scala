package com.cjs.sparkLearn.rddLearn.PartitionLearn

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object SelfPartitioner {

  class MyPartitionar(partitions: Int) extends Partitioner {
    override def numPartitions: Int = partitions //分区数

    override def getPartition(key: Any): Int = {
      println(key)
      key.hashCode() % numPartitions
    }
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SelfPartitioner").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd1: RDD[(Int, String)] = sc.parallelize(Array((1, "aaa"), (2, "bbb"), (3, "ccc"), (4, "ddd")))

    val value: RDD[(Int, String)] = rdd1.partitionBy(new MyPartitionar(3))

    //result:List(List((3,ccc)), List((1,aaa), (4,ddd)), List((2,bbb)))
    println(value.glom().map(_.toList).collect().toList)

  }
}

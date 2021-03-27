package com.cjs.sparkLearn.rddLearn.IOLearn

import org.apache.spark.{SparkConf, SparkContext}

object RDDSaveLearn {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local[*]").setAppName("RDDSaveLearn")
    val sc = new SparkContext(sparConf)

    //rdd存储
    val rdd = sc.makeRDD(
      List(
        ("a", 1),
        ("b", 2),
        ("c", 3)
      )
    )

    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")
    //saveAsSequenceFile只有key:value形式的才能保存
    rdd.saveAsSequenceFile("output3")

    //ｒｄｄ读取
    //    val rdd = sc.textFile("output1")
    //    println(rdd.collect().mkString(","))
    //
    //    val rdd1 = sc.objectFile[(String, Int)]("output2")
    //    println(rdd1.collect().mkString(","))
    //
    //    val rdd2 = sc.sequenceFile[String, Int]("output3")
    //    println(rdd2.collect().mkString(","))

    sc.stop()
  }

}

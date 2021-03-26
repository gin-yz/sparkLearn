//类似于 SQL 语句的左外连接,返回(key,(value1,value2)),若无匹配到的，ｖａｌｕｅ２为Ｎｏｎｅ
package com.cjs.sparkLearn.rddLearn.KeyValueLearn

import org.apache.spark.{SparkConf, SparkContext}

object LeftOuterJoinLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("LeftOuterJoin").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataRDD1 = sc.makeRDD(List(("a",1),("b",2),("c",3)))
    val dataRDD2 = sc.makeRDD(List(("a",1),("b",2),("c",3),("d",4)))

    val value = dataRDD1.leftOuterJoin(dataRDD2).map(item=>(item._1,(item._2._1,item._2._2.getOrElse(None)))).collect()

    println(value.toList)

  }

}

package com.cjs.sparkLearn.graphxLearn

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object InitDemo {

  def main(args: Array[String]): Unit = {
    val sparConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GraphxLearn").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc: SparkContext = new SparkContext(sparConf)

    //构造点集合，(index,(k,v))
    val rdd1: RDD[(Long, (String, Int))] = sc.makeRDD(Seq(
      (1L, ("李连杰", 56)),
      (2L, ("王祖贤", 53)),
      (6L, ("成龙", 62)),
      (9L, ("周星驰", 57)),
      (133L, ("周润发", 62)),
      (138L, ("岳云鹏", 33)),
      (16L, ("沈腾", 36)),
      (21L, ("黄渤", 45)),
      (44L, ("杨幂", 33)),
      (158L, ("张云雷", 28)),
      (5L, ("高圆圆", 42)),
      (7L, ("张卫健", 55))
    ))

    // 构造边的集合
    val edgeRDD: RDD[Edge[Int]] = sc.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))

    // 构造图
    val graph: Graph[(String, Int), Int] = Graph(rdd1, edgeRDD)
    // 取出顶点 (6,1),(2,1)
    val common: VertexRDD[VertexId] = graph.connectedComponents().vertices
    //common.foreach(println)
    common.join(rdd1)
      .map { case (userId, (cmId, (name, age))) =>
        (cmId, List((name, age)))
      }
      .reduceByKey(_ ++ _)
      .foreach(println)

  }

}

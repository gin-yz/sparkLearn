//取出每个分区相同 key 对应值的最大值，然后相加
package com.cjs.sparkLearn.rddLearn.KeyValueLearn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKeyLearn {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("GroupByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //Array(Array((a,3), (a,2), (c,4)), Array((b,3), (c,6), (c,8)))
    val rdd: RDD[(String, Int)] = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
    //输出各个组下的元组
    println(rdd.glom().map(_.toList).collect().toList)
    //不管是操作１还是操作２，都是基于将ｋｅｙ进行ｇｒｏｕｐ　ｂｙ之后，同一个ｋｅｙ中的ｖａｌｕｅ进行的操作．
    //先是各个分区里面的(key,value)中的key对应的value比大小，然后再是分区间key对应的value相加
    //value1的初始值为zeroValue为１,在aggregateByKey中只参与第一次运算，第二次的相加操作不参与！！！
    val value: RDD[(String, Int)] = rdd.aggregateByKey(Int.MinValue)((value1, value2) => Math.max(value1, value2), (value1, value2) => value1 + value2)

    println(value.collect().toList)

  }

}

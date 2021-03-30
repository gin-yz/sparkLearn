package com.cjs.sparkLearn.rddLearn.ExampleLearn

import scala.collection.mutable

object Test {
  def main(args: Array[String]): Unit = {
    val ids: List[Long] = List[Long](1,2,3,4,5,6,7)

    ids.zip(ids.slice(1,ids.length)).foreach(println)
  }

}

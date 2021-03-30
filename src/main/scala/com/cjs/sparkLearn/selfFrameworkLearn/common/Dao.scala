package com.cjs.sparkLearn.selfFrameworkLearn.common

import org.apache.spark.rdd.RDD

trait Dao {
  def readFiles(path: String): RDD[Any]

}

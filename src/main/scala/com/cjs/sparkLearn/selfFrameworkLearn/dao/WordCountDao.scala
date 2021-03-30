package com.cjs.sparkLearn.selfFrameworkLearn.dao

import com.cjs.sparkLearn.selfFrameworkLearn.common.Dao
import org.apache.spark.rdd.RDD

class WordCountDao extends Dao{
  override def readFiles(path: String): RDD[Any] = {
    ???
  }
}

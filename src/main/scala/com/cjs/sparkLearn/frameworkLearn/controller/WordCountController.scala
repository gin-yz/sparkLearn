package com.cjs.sparkLearn.frameworkLearn.controller

import com.cjs.sparkLearn.frameworkLearn.common.TController
import com.cjs.sparkLearn.frameworkLearn.service.WordCountService

/**
  * 控制层
  */
class WordCountController extends TController {

    private val wordCountService = new WordCountService()

    // 调度
    def dispatch(): Unit = {
        // TODO 执行业务操作
        val array = wordCountService.dataAnalysis()
        array.foreach(println)
    }
}

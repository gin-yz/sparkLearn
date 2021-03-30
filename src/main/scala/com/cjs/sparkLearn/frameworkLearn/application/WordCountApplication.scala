package com.cjs.sparkLearn.frameworkLearn.application

import com.cjs.sparkLearn.frameworkLearn.common.TApplication
import com.cjs.sparkLearn.frameworkLearn.controller.WordCountController


object WordCountApplication extends App with TApplication{

    // 启动应用程序
    start(){
        val controller = new WordCountController()
        controller.dispatch()
    }

}

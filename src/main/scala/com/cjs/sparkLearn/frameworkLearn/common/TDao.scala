package com.cjs.sparkLearn.frameworkLearn.common

import com.cjs.sparkLearn.frameworkLearn.util.EnvUtil


trait TDao {

    def readFile(path:String) = {
        EnvUtil.take().textFile(path)
    }
}

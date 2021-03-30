package com.cjs.sparkLearn.testDemo

import java.lang.reflect.Constructor

object Demo1{

  class Fruits(val id: Int, val name: String) {
    var age: Int = _

    def this(id: Int, name: String, age: Int) {
      this(id, name)
      this.age = age
    }

    def func(): Unit = {
      println("func invoke")
    }
  }


  def main(args: Array[String]): Unit = {
    val cjs: Fruits = new Fruits(1, "cjs", 22)

//    val value: Constructor[_ <: Fruits] = cjs.getClass.getConstructor(classOf[Int],classOf[String],classOf[Int])

//    val fruits: Fruits = value.newInstance(1, "cjsdsg",23)

//    println(fruits)
  }

}

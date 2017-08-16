package com.cpc.spark.ml

import org.apache.spark.sql.SparkSession
import org.luaj.vm2.lib.jse.JsePlatform

import scala.io.Source

/**
  * Created by roydong on 10/08/2017.
  */
object TestLua {

  def main(args: Array[String]): Unit = {

    //val luafiles = Source.fr
    val g = JsePlatform.standardGlobals()

    g.load("lua/ml/feature/parser.lua")
    val ret = g.loadfile("lua/ml/feature/parser.lua")

    println(ret)

  }
}


package com.cpc.spark.ml

import java.util.Date

import com.cpc.spark.log.parser.UnionLog
import mlserver.mlserver._
import org.apache.spark.sql.SparkSession
import org.luaj.vm2.LuaValue
import org.luaj.vm2.lib.jse.JsePlatform
import com.cpc.spark.ml.ctrmodel.v2.FeatureParser

import scala.io.Source

/**
  * Created by roydong on 10/08/2017.
  */
object TestLua {

  def main(args: Array[String]): Unit = {

    FeatureParser.loadLua("lua/ml/feature/")
    val ret = FeatureParser.parseByLua(UnionLog())
    println(ret)
  }
}




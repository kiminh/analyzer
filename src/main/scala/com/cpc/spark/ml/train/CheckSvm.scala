
package com.cpc.spark.ml.train

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.parser.UnionLog
import com.cpc.spark.ml.parser.{FeatureParser, UserClick}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random


/*
样本
 */
object CheckSvm {

  def main(args: Array[String]): Unit = {
    if (args.length < 0) {
      System.err.println(
        s"""
           |Usage: GenerateAdvSvm <version:string> <daybefore:int> <days:int> <rate:int>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger().setLevel(Level.WARN)
    val ctx = SparkSession.builder()
      .appName("check svm")
      .getOrCreate()

    ctx.read
      .text("/user/cpc/svmdata/v18/2017-06-18")
      .rdd
      .map(_.getString(0).split(" "))
      .toLocalIterator
      //.take(100)
      .foreach {
        x =>
          var p = -1
          x.foreach {
            row =>
              val v = row.split(":")
              if (v.length == 2) {
                if (v(0).toInt < p) {
                  throw new Exception("error:" + x.mkString(" "))
                }
                p = v(0).toInt
              }
          }
      }

    ctx.stop()
  }
}


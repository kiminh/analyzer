package com.cpc.spark.ml.train

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ml.parser.UserClick
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by roydong on 06/06/2017.
  */
object SumUidClkV2 {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: GenerateAdvSvm <day_before:int> <days:int>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger().setLevel(Level.WARN)
    val dayBefore = args(0).toInt
    val days = args(1).toInt
    val ctx = SparkSession.builder()
      .appName("sum uid clk")
      .enableHiveSupport()
      .getOrCreate()

    val uc = new UserClick("cpc-bj05", 6381, 5)
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    for (n <- 1 to days) {
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      println("get data " + date)
      val n = uc.sumUnionLog(ctx, date, "")
      println("done", date, n)
      cal.add(Calendar.DATE, 1)
    }
    ctx.stop()
    println("saving to redis")
    uc.saveToRedis()
    println("done")
  }
}

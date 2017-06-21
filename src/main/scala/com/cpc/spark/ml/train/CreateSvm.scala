package com.cpc.spark.ml.train

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.parser.UnionLog
import com.cpc.spark.ml.parser.{FeatureParser, UserClick, UserClickTrait}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random


/*
样本
 */
object CreateSvm extends UserClickTrait {

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println(
        s"""
           |Usage: create svm <version:string> <daybefore:int> <days:int> <rate:int> <hour:string>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger().setLevel(Level.WARN)
    val version = args(0)
    val dayBefore = args(1).toInt
    val days = args(2).toInt
    val rate = args(3).toInt
    val hour = args(4)
    val ctx = SparkSession.builder()
      .appName("GenerateAdvSvm " + version)
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    for (n <- 1 to days) {
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      println("get data " + date)

      var hourSql = ""
      if (hour.length > 0) {
        hourSql = "and `hour` in (\"%s\")".format(hour.split(",").mkString("\",\""))
      }

      val rawlog = ctx.sql(
        s"""
           |select * from dl_cpc.cpc_union_log where `date` = "%s" %s and isfill = 1 and adslotid > 0
        """.stripMargin.format(date, hourSql))
        .as[UnionLog].rdd
        .filter {
          u =>
            var ret = false
            if (u != null && u.searchid.length > 0 && u.uid.length > 0) {
              if (u.media_appsid == "80000001" || u.media_appsid == "80000002") {
                //1 / 20 负样本
                if (u.isclick == 1 || Random.nextInt(rate) == 0) {
                  ret = true
                }
              }
            }
            ret
        }
        .cache()


      var clickRdd: rdd.RDD[(UnionLog, Int, Int, Int, Int, Int)] = null

      loadUserClickFromFile()

      clickRdd = rawlog.mapPartitions {
        p =>

          p.map {
            u =>
              val clk = userClk.getOrElse(u.uid, 0)
              val pv = userPV.getOrElse(u.uid, 0)
              val ad = userAdClick.getOrElse("%s-%d".format(u.uid, u.ideaid), 0)
              val slot = userSlotClick.getOrElse("%s-%s".format(u.uid, u.adslotid), 0)
              val slotAd = userSlotAdClick.getOrElse("%s-%s-%d".format(u.uid, u.adslotid, u.ideaid), 0)

              (u, clk, pv, ad, slot, slotAd)
          }
      }

      if (n == 1) {
        clickRdd.filter(_._6 > 0).take(20).foreach {
          x =>
            println(x._1.uid, x._2, x._3, x._4, x._5, x._6)
        }
      }

      println("add click data done")

      clickRdd
        .map {
          x =>
            val pv = 1D
            FeatureParser.parseUnionLog(x._1,
              x._2.toDouble / pv, x._3,
              x._4.toDouble / pv,
              x._5.toDouble / pv,
              x._6.toDouble / pv)
        }
        .toDF()
        .write
        .mode(SaveMode.Overwrite)
        .text("/user/cpc/svmdata/" + version + "/" + date)

      println("done", rawlog.count())
      rawlog.unpersist()
      clickRdd.unpersist()
      cal.add(Calendar.DATE, 1)
    }

    ctx.stop()
  }
}


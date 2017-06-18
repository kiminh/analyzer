package com.cpc.spark.ml.train

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.parser.UnionLog
import com.cpc.spark.ml.parser.{FeatureParser, FeatureParserV2, UserClick}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random


/*
样本
 */
object AdvSvm extends UserClick {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println(
        s"""
           |Usage: GenerateAdvSvm <version:string> <daybefore:int> <days:int> <rate:int>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger().setLevel(Level.WARN)
    val version = args(0)
    val dayBefore = args(1).toInt
    val days = args(2).toInt
    val rate = args(3).toInt
    val ctx = SparkSession.builder()
      .appName("GenerateAdvSvm " + version)
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._
    val conf = ConfigFactory.load()

    println("read user info")
    loadUserClickFromFile()
    println("done", userClk.size, userPV.size, userAdClick.size, userSlotClick.size, userSlotAdClick.size)

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    for (n <- 1 to days) {
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      println("get data " + date)

      val rawlog = ctx.sql(
        s"""
           |select * from dl_cpc.cpc_union_log where `date` = "%s" and isfill = 1 and adslotid > 0
        """.stripMargin.format(date))
        .as[UnionLog].rdd
        .filter {
          u =>
            var ret = false
            if (u != null && u.searchid.length > 0 && u.uid.length > 0) {
              //TODO network数据不准确暂时忽略
              if (u.sex > 0 && u.coin > 0 && u.age > 0 && u.os > 0) {
                //1 / 20 负样本
                if (u.isclick == 1 || Random.nextInt(rate) == 0) {
                  ret = true
                }
              }
            }
            ret
        }
        .cache()

      rawlog
        .mapPartitions {
          p =>
            p.map {
              u =>
                val ad = userAdClick.getOrElse("%s-%d".format(u.uid, u.ideaid), 0)
                val slot = userSlotClick.getOrElse("%s-%s".format(u.uid, u.adslotid), 0)
                val slotAd = userSlotAdClick.getOrElse("%s-%s-%d".format(u.uid, u.adslotid, u.ideaid), 0)
                FeatureParser.parseUnionLog(u, userClk.getOrElse(u.uid, 0), userPV.getOrElse(u.uid, 0), ad, slot, slotAd)
            }
        }
        .toDF()
        .write
        .mode(SaveMode.Overwrite)
        .text("/user/cpc/svmdata/" + version + "/" + date)

      println("done", rawlog.count())
      rawlog.unpersist()
      cal.add(Calendar.DATE, 1)
    }

    ctx.stop()
  }
}


package com.cpc.spark.qukan.anal

import java.text.SimpleDateFormat
import java.util.Calendar
import com.cpc.spark.qukan.parser.HdfsParser
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by Roy on 2017/4/17.
  */
object AnalUserProfile {

  def main(args: Array[String]): Unit = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val date = new SimpleDateFormat("yyyyMMdd").format(cal.getTime)

    val ctx = SparkSession.builder()
      .appName("cpc anal user profile [%s]".format(day))
      .enableHiveSupport()
      .getOrCreate()

    //年龄 0: 未知 1: 小于18 2:18-23 3:24-30 4:31-40 5:41-50 6: >50
    val age = new Array[Int](7)
    //性别 0: 未知 1: 男 2: 女
    val sex = new Array[Int](3)
    val coin = new Array[Int](20)
    var total = 0
    val profilePath = "/warehouse/rpt_qukan.db/device_member_coin/thedate=%s".format(day)
    ctx.read.text(profilePath).rdd
      .map(x => HdfsParser.parseTextRow(x.getString(0)))
      .filter(x => x != null && x.devid.length > 0)
      .map(x => (x.devid, x))
      .reduceByKey((x, y) => y)
      .map(_._2)
      .toLocalIterator
      .foreach {
        x =>
          age(x.age) = age(x.age) + 1
          sex(x.sex) = sex(x.sex) + 1
          val l = x.coin.toString.length
          if (l < coin.length) {
            coin(l - 1) = coin(l - 1) + 1
          }
          total = total + 1
      }

    var stats = List[(String, Int)]()
    for (i <- 0 to sex.length - 1) {
      stats :+= ("sex_" + i.toString, sex(i))
    }
    for (i <- 0 to age.length - 1) {
      stats :+= ("age_" + i.toString, age(i))
    }
    for (i <- 0 to coin.length - 1) {
      stats :+= ("coin_" + i.toString, coin(i))
    }
    stats :+= ("total", total)
    stats :+= ("date", date.toInt)

    ctx.createDataFrame(stats)
      .write
      .mode(SaveMode.Append)
      .saveAsTable("dl_cpc.user_profile_stats")

    /*
    println("年龄 0: 未知 1: 小于18 2:18-23 3:24-30 4:31-40 5:41-50 6: >50")
    for (i <- 0 to age.length - 1) {
      println("age %d: %d %.4f".format(i, age(i), age(i).toFloat / total.toFloat))
    }

    println("\n性别 0: 未知 1: 男 2: 女")
    for (i <- 0 to sex.length - 1) {
      println("sex %d: %d %.4f".format(i, sex(i), sex(i).toFloat / total.toFloat))
    }

    println("\n积分, 不显示的分段为0个")
    for (i <- 0 to coin.length - 1) {
      if (coin(i) > 0) {
        println("分段 %d: %d %.4f".format(i + 1, coin(i), coin(i).toFloat / total.toFloat))
      }
    }

    println("\ntotal %d".format(total))
    */

    ctx.stop()
  }
}

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
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: AnalUserProfile <table>
           |
        """.stripMargin)
      System.exit(1)
    }
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val date = new SimpleDateFormat("yyyyMMdd").format(cal.getTime)

    val ctx = SparkSession.builder()
      .appName("cpc anal user profile [%s]".format(day))
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    //年龄 0: 未知 1: 小于18 2:18-23 3:24-30 4:31-40 5:41-50 6: >50
    val age = new Array[Int](7)
    //性别 0: 未知 1: 男 2: 女
    val sex = new Array[Int](3)
    val coin = new Array[Int](15)
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

    val ages = new Array[String](7)
    val sexs = new Array[String](3)
    val coins = new Array[String](15)

    for (i <- 0 to age.length - 1) {
      ages(i) = "%d(%.4f)".format(age(i), age(i).toFloat / total.toFloat)
    }
    for (i <- 0 to sex.length - 1) {
      sexs(i) = "%d(%.4f)".format(sex(i), sex(i).toFloat / total.toFloat)
    }
    for (i <- 0 to coin.length - 1) {
      if (coin(i) > 0) {
        coins(i) = "%d(%.4f)".format(coin(i), coin(i).toFloat / total.toFloat)
      } else {
        coins(i) = "%d(0)".format(coin(i))
      }
    }

    val stats = UserProfileStats(
      sex = sexs.mkString(" "),
      age = ages.mkString(" "),
      coin = coins.mkString(" "),
      total = total,
      date = day
    )

    Seq(stats)
      .toDF()
      .write
      .mode(SaveMode.Append)
      .saveAsTable("dl_cpc." + args(0))

    ctx.stop()
  }
}


case class UserProfileStats (
                            sex: String = "",
                            age: String = "",
                            coin: String = "",
                            total: Int = 0,
                            date: String = ""
                            )

package com.cpc.spark.qukan.interest

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.parser.UnionLog
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by roydong on 08/11/2017.
  */
object InterestedCategory {

  def main(args: Array[String]): Unit = {

    val adclass = args(0).toInt

    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("cpc get user interested category")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._


    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -3)

    var date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val day1 = spark.sql(
      """
        |select * from dl_cpc.cpc_union_log where `date` = "%s"
        |and round(ext['adclass'].int_value / 1e6,0) = %d and isshow = 1
      """.stripMargin.format(date, adclass)).as[UnionLog].rdd
      .map {
        u =>
          (u.uid, u.isclick)
      }

    cal.add(Calendar.DATE, 1)
    date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val day2 = spark.sql(
      """
        |select * from dl_cpc.cpc_union_log where `date` = "%s"
        |and round(ext['adclass'].int_value / 1e6,0) = %d and isshow = 1
      """.stripMargin.format(date, adclass)).as[UnionLog].rdd
      .map {
        u =>
          (u.uid, u.isclick)
      }

    val user = day1.leftOuterJoin(day2)
      .map {
        x =>
          val c1 = x._2._1
          var c2 = 0
          var s2 = 0

          if (x._2._2.isDefined) {
            c2 = x._2._2.get
            s2 = 1
          }

          (x._1, (c1, 1, c2, s2))
      }
      .reduceByKey {
        (x, y) =>
          (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4)
      }
      .filter(_._2._1 > 0)
      .map {
        x =>
          x._2
      }
      .cache()

    val ctr = user.reduce {
        (x, y) =>
          (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4)
      }


    println(user.count(), user.filter(_._4 > 0).count())
    println(ctr._1.toDouble / ctr._2.toDouble)
    println(ctr._3.toDouble / ctr._4.toDouble)



    /*
    cal.add(Calendar.DATE, 1)
    date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val day3 = spark.sql(
      """
        |select * from dl_cpc.cpc_union_log where `date` = "%s"
        |and ext['adclass'].int_value = %d and isshow = 1
      """.stripMargin.format(date, adclass)).as[UnionLog].rdd
      .map {
        u =>
          (u.uid, u.isclick)
      }
      */


  }
}

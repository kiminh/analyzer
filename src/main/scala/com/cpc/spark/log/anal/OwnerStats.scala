package com.cpc.spark.log.anal

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by roydong on 25/07/2018.
  */
object OwnerStats {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ad owner stats")
      .enableHiveSupport()
      .getOrCreate()

    val days = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -days)

    val stmt =
      """
        |select searchid, ext_int["lx_package"], isshow, isclick, ext_int["siteid"] from dl_cpc.cpc_union_log
        |where `date` >= "2018-07-31" and isshow > 0 and adsrc <= 1 and ext_int["siteid"] in (30248, 30250)
      """.stripMargin

    println(stmt)
    val ulog = spark.sql(stmt)
      .rdd
      .map {
        x =>
          val sid = x.getString(0)
          val dtu = x.getInt(1)
          val isshow = x.getInt(2)
          val isclick = x.getInt(3)
          val siteid = x.getLong(4)
          (sid, dtu, isshow, isclick, siteid)
      }

    val tlog = spark.sql(
      """
        |select searchid,trace_type from dl_cpc.cpc_union_trace_log where `date` >= "2018-07-31"
      """.stripMargin).rdd
      .map {
        x =>
          val sid = x.getString(0)
          val tt = x.getString(1)
          if (tt == "active5") {
            (sid, (1, 0))
          } else if (tt == "active7") {
            (sid, (0, 1))
          } else {
            null
          }
      }
      .filter(_ != null)
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    ulog.map(x => (x._1, x))
      .leftOuterJoin(tlog)
      .map {
        x =>
          val p1 = x._2._1
          val dtu = p1._2
          val isshow = p1._3
          val isclick = p1._4
          val siteid = p1._5
          var a5 = 0
          var a7 = 0
          if (x._2._2.isDefined) {
            val v = x._2._2.get
            if (v._1 > 0) {
              a5 = 1
            }
            if (v._2 > 0) {
              a7 = 1
            }
          }

          val key = (dtu, siteid)
          (key, (isshow, isclick, a5, a7))
      }
      .reduceByKey{
        (x, y) =>
          (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4)
      }
      .toLocalIterator
      .foreach {
        x =>
          val p1 = x._1
          val p2 = x._2
          println(p1._1, p1._2, p2._1, p2._2, p2._3, p2._4)
      }
  }
}

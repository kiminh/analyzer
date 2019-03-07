package com.cpc.spark.small.tool

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wanli on 2017/7/24.
  */
object GetDeviceContentInfo {

  def main(args: Array[String]): Unit = {
    val dayBefore = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val num = args(1).toInt
    val vn = args(2).toInt

    println("small tool GetDeviceContentInfo running ... %s num %d vn %d".format(day,num,vn))

    val ctx = SparkSession.builder()
      .appName("small tool GetDeviceContentInfo runing ... %s num %d vn %d".format(day,num,vn))
      .enableHiveSupport()
      .getOrCreate()

    val deviceContentRddSort =
      ctx.sql("SELECT device,content_type,read_pv FROM rpt_qukan.rpt_qukan_device_content_type_read_pv WHERE thedate='%s'".format(day)).rdd
        .filter(_.getInt(2) > num)
        .map { x =>
          (x.getString(0), x.getString(1))
        }
        .groupBy(_._1)
        .map {
          x =>
            (x._1, (x._2, 0.toLong, 0.toLong, 0.toLong))
        }

    val unionLogRdd =
      ctx.sql(
        s"""
           |SELECT
           |  uid
           |  ,count(*)
           |  ,sum(isshow)
           |  ,sum(isclick)
           |FROM dl_cpc.cpc_basedata_union_events
           |WHERE day='%s'
           |  AND media_appsid in ('80000001', '80000002', '80000006')
           |GROUP BY uid
         """.stripMargin.format(day)).rdd
        .map {
          x => {
            (x.getString(0), (Iterable(("", "")), x.getLong(1), x.getLong(2), x.getLong(3)))
          }
        }

    unionLogRdd.union(deviceContentRddSort)
      .reduceByKey {
        (a, b) =>
          if (a._1.head._1 == null || a._1.head._1.length > 0) {
            (a._1, a._2 + b._2, a._3 + b._3, a._4 + b._4)
          } else {
            (b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4)
          }

      }
      .map {
        x =>
          var tmp = x._2._1.toSeq.map {
            y =>
              (y._2, x._2._2, x._2._3, x._2._4)
          }
          (x._1, tmp)
      }
      .flatMap {
        x =>
          val ans = new ArrayBuffer[(String, (Long, Long, Long))]()
          for (y <- x._2) {
            ans += ((y._1, (y._2, y._3, y._4)))
          }
          ans
      }
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
      .map {
        x =>
          (x._1, x._2._1, x._2._2, x._2._3)
      }
      .saveAsTextFile("/user/cpc/wl/small-tool-GetDeviceContentInfo-%s-v%d".format(day,vn))
    ctx.stop()
  }

}
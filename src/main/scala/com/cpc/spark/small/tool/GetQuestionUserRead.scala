package com.cpc.spark.small.tool

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession

/**
  * Created by wanli on 2017/8/9.
  */
object GetQuestionUserRead {

  def main(args: Array[String]): Unit = {
    val dataStartDay = args(0).toInt
    val dataEndDay = args(1).toInt

    val scal = Calendar.getInstance()
    scal.add(Calendar.DATE, -dataStartDay)
    val dataStart = new SimpleDateFormat("yyyy-MM-dd").format(scal.getTime)

    val ecal = Calendar.getInstance()
    ecal.add(Calendar.DATE, -dataEndDay)
    val dataEnd = new SimpleDateFormat("yyyy-MM-dd").format(ecal.getTime)

    val ctx = SparkSession.builder()
      .appName("small tool GetQuestionUserRead run ...%s-%s".format(dataStart, dataEnd))
      .enableHiveSupport()
      .getOrCreate()

    val qukanLogCmd = ctx.sql(
      """
        |SELECT DISTINCT qkc.device,qc.title
        |from rpt_qukan.qukan_log_cmd qkc
        |INNER JOIN  gobblin.qukan_content qc ON qc.id=qkc.content_id
        |WHERE qkc.cmd=300 AND qkc.thedate>="%s" AND qkc.thedate<="%s" AND qkc.member_id IS NOT NULL
        |AND qkc.device IS NOT NULL
        |""".stripMargin.format(dataStart, dataEnd))
      .rdd
      .map {
        x =>
          (x.getString(0), x.getString(1))
      }
      .groupBy(_._1)
      .map {
        x =>
          (x._1, (x._1, x._2.map(_._2).toSeq, ""))
      }
      .filter(_._2._2.length > 3)

    val unionLog = ctx.sql(
      """
        |SELECT DISTINCT uid
        |FROM dl_cpc.cpc_union_log
        |WHERE `date`="%s" AND sex=0 AND uid IS NOT NULL
      """.stripMargin.format(dataEnd)).rdd
      .map {
        x =>
          (x.getString(0), (x.getString(0), Seq(""), "x"))
      }

    val tmp = unionLog
      .union(qukanLogCmd)
      .reduceByKey {
        //deviceid
        (a, b) =>
          var xa = b
          if (a._2.length > 3) {
            xa = a
          }
          (xa._1, xa._2, a._3 + b._3)
      }
      .filter {
        x =>
          (x._2._3 == "x") && (x._2._2.length > 3)
      }
      .cache()

    tmp
      .map {
        x =>
          "%s\t%s".format(x._2._1, x._2._2.mkString("$|$"))
      }
      .saveAsTextFile("/user/cpc/wl/work/small-tool-GetQuestionUserRead/%s".format(dataEnd))

    println("count", tmp.count())


  }

}

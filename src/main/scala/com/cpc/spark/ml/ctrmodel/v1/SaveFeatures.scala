package com.cpc.spark.ml.ctrmodel.v1

import java.util.Date

import com.cpc.spark.log.parser.{TraceLog, UnionLog}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
  * Created by roydong on 15/12/2017.
  */
object SaveFeatures {

  def main(args: Array[String]): Unit = {
    val date = args(0)

    val spark = SparkSession.builder()
      .appName("save feature ids" + date)
      .enableHiveSupport()
      .getOrCreate()

    //saveDataFromLog(spark, date)
    saveCvrData(spark, date)
  }

  def saveDataFromLog(spark: SparkSession, date: String): Unit = {
    val stmt =
      """
        |select searchid,isclick as label,sex,age,os,isp,network,
        |       city,media_appsid,ext['phone_level'].int_value as phone_level,`timestamp`,adtype,
        |       planid,unitid,ideaid,ext['adclass'].int_value as adclass,adslotid,
        |       adslot_type,ext['pagenum'].int_value as pagenum,ext['bookid'].string_value as bookid
        |
        |from dl_cpc.cpc_union_log where `date` = "%s" and isshow = 1
        |and ext['antispam'].int_value = 0
        |
      """.stripMargin.format(date)

    println(stmt)
    val ulog = spark.sql(stmt)
    val num = ulog.count().toDouble
    var rate = 1d
    //最多1亿条数据
    if (num > 1e8) {
      rate = 1e8 / num
    }
    println("num", num, rate)
    ulog.randomSplit(Array(rate, 1 - rate), new Date().getTime)(0)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/user/cpc/lrmodel/ctrdata/%s".format(date))
  }

  def saveCvrData(spark: SparkSession, date: String): Unit = {
    import spark.implicits._
    val cvrlog = spark.sql(
      s"""
         |select * from dl_cpc.cpc_union_trace_log where `date` = "%s"
        """.stripMargin.format(date))
      .as[TraceLog].rdd
      .map {
        x =>
          (x.searchid, Seq(x))
      }
      .reduceByKey(_ ++ _)
      .map {
        x =>
          val convert = cvrPositive(x._2)
          (x._1, convert)
      }
      .toDF("searchid", "label")
    println("cvr log", cvrlog.count())

    val sqlStmt =
      """
        |select searchid,sex,age,os,isp,network,
        |       city,media_appsid,ext['phone_level'].int_value as phone_level,`timestamp`,adtype,
        |       planid,unitid,ideaid,ext['adclass'].int_value as adclass,adslotid,
        |       adslot_type,ext['pagenum'].int_value as pagenum,ext['bookid'].string_value as bookid
        |
        |from dl_cpc.cpc_union_log where `date` = "%s" and isclick = 1
        |and ext['antispam'].int_value = 0
        |
      """.stripMargin.format(date)
    println(sqlStmt)
    val clicklog = spark.sql(sqlStmt)
    println("click log", clicklog.count())

    clicklog.join(cvrlog, Seq("searchid"))
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/user/cpc/lrmodel/cvrdata/%s".format(date))
  }

  def cvrPositive(traces: Seq[TraceLog]): Int = {
    var stay = 0
    var click = 0
    var active = 0
    var mclick = 0
    traces.foreach {
      t =>
        t.trace_type match {
          case s if s.startsWith("active") => active += 1

          case "buttonClick" => click += 1

          case "clickMonitor" => mclick += 1

          case "inputFocus" => click += 1

          case "press" => click += 1

          case "stay" =>
            if (t.duration > stay) {
              stay = t.duration
            }

          case _ =>
        }
    }

    if ((stay >= 30 && click > 0) || active > 0 || (stay >= 60 && mclick > 0)) {
      1
    } else {
      0
    }
  }
}

package com.cpc.spark.log.anal

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.parser.LogParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.sys.process._

/**
  * Created by Roy on 2017/4/18.
  */
@deprecated
object AnalCfgLog {

  //  var srcRoot = "/gobblin/source/cpc"
  var srcRoot = "/warehouse/dl_cpc.db"

  val partitionPathFormat = new SimpleDateFormat("yyyy-MM-dd/HH")

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        s"""
           |usage: analunionlog <hdfs_input>  <hour>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    srcRoot = args(0)
    val hourBefore = args(1).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -hourBefore)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val hour = new SimpleDateFormat("HH").format(cal.getTime)
    val table = "cpc_cfg_log"
    val spark = SparkSession.builder()
      .appName("cpc anal cfg log %s partition = %s".format(table, partitionPathFormat.format(cal.getTime)))
      .enableHiveSupport()
      .getOrCreate()
    //    val cfgData = prepareSource(spark, "cpc_cfg", hourBefore, 1)
    val cfgData = prepareSourceString(spark, "cpc_cfg", "src_cpc_cfg_minute", hourBefore, 1)
    if (cfgData == null) {
      spark.stop()
      System.exit(1)
    }
    val cfglog = cfgData.map(x => LogParser.parseCfgLog_v2(x)).filter(x => x != null).map(x => x.copy(date = date, hour = hour))
    //clear dir
    //    Utils.deleteHdfs("/warehouse/dl_cpc.db/%s/date=%s/hour=%s".format(table, date, hour))
    //    spark.createDataFrame(cfglog)
    //      .write
    //      .mode(SaveMode.Append)
    //      .format("parquet")
    //      .partitionBy("date", "hour")
    //      .saveAsTable("dl_cpc." + table)
    //    println("cfglog", cfgData.count())


    spark.createDataFrame(cfglog)
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/warehouse/dl_cpc.db/%s/date=%s/hour=%s".format(table, date, hour))
    spark.sql(
      """
        |ALTER TABLE dl_cpc.%s add if not exists PARTITION(`date` = "%s", `hour` = "%s")
        | LOCATION  '/warehouse/dl_cpc.db/%s/date=%s/hour=%s'
      """.stripMargin.format(table, date, hour, table, date, hour))
    println("cfglog", cfglog.count())

    //输出标记文件
    s"hadoop fs -touchz /user/cpc/okdir/cpc_cfg_log_done/$date-$hour.ok" !

    spark.stop()
    for (i <- 0 to 100) {
      println("-")
    }
    println("AnalCfgLog_done")
  }


  def prepareSourceString(ctx: SparkSession, key: String, src: String, hourBefore: Int, hours: Int): rdd.RDD[String] = {
    val input = "%s/%s/%s/*".format(srcRoot, src, getDateHourPath(hourBefore, hours)) ///gobblin/source/cpc/cpc_search/{05,06...}
    println(input)
    ctx.read
      .parquet(input)
      .repartition(1000)
      .rdd
      .flatMap {
        r =>
          //val s = r.getMap[String, Row](2).getOrElse(key, null)
          val s = r.getAs[Map[String, Row]]("field").getOrElse(key, null)
          val s1 = r.getAs[Map[String, Row]]("field").getOrElse(key + "_new", null)

          val r1 = if (s == null) {
            null
          } else {
            s.getAs[String]("string_type")
          }
          val r2 = if (s1 == null) {
            null
          } else {
            s1.getAs[String]("string_type")
          }
          Seq(r1, r2)
      }
      .filter(_ != null)
  }


  def getDateHourPath(hourBefore: Int, hours: Int): String = {
    val cal = Calendar.getInstance()
    val parts = new Array[String](hours)
    cal.add(Calendar.HOUR, -hourBefore)
    for (h <- 0 until hours) {
      parts(h) = partitionPathFormat.format(cal.getTime)
      cal.add(Calendar.HOUR, 1)
    }
    "{" + parts.mkString(",") + "}"
  }
}



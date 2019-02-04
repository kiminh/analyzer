package com.cpc.spark.log.anal

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.parser.LogParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.sys.process._

/**
  * Created by Roy on 2017/4/18.
  */
object AnalCfgLogV2 {


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
    val date = args(0)
    val hour = args(1)
    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -2)
    val table = "cpc_cfg_log"
    val spark = SparkSession.builder()
      .appName("cpc anal cfg log %s partition = %s".format(table, partitionPathFormat.format(cal.getTime)))
      .enableHiveSupport()
      .getOrCreate()

    val cfgData = spark.sql(
      s"""
         |select raw
         |from dl_cpc.cpc_basedata_cfg_log
         |where day = '$date' and hour = '$hour'
       """.stripMargin
    )

    val cfglog = cfgData.repartition(100)
      .rdd
      .map { x =>
        val raw = x.getAs[String]("raw")
        LogParser.parseCfgLog_v2(raw)
      }
      .filter(x => x != null)
      .map(x => x.copy(date = date, hour = hour))


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


}



package com.cpc.spark.ocpc.report

import java.util.Properties
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object OcpcDataSummary {
  var mariadb_write_url = ""
  val mariadb_write_prop = new Properties()

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val data = spark
      .table("dl_cpc.ocpc_summary_report_hourly")
      .where(s"`date`='$date' and `hour`='$hour'")
    saveDataSummaryToReport(data, spark)
  }


  def saveDataSummaryToReport(data: DataFrame, spark: SparkSession) = {
    val conf = ConfigFactory.load()
    val tableName = "report2.report_ocpc_data_summary_v2"
    mariadb_write_url = conf.getString("mariadb.report2_write.url")
    mariadb_write_prop.put("user", conf.getString("mariadb.report2_write.user"))
    mariadb_write_prop.put("password", conf.getString("mariadb.report2_write.password"))
    mariadb_write_prop.put("driver", conf.getString("mariadb.report2_write.driver"))

    println("#################################")
    println("count:" + data.count())
    println("url: " + conf.getString("mariadb.report2_write.url"))
    println("table name: " + tableName)
    println("user: " + conf.getString("mariadb.report2_write.user"))
    println("password: " + conf.getString("mariadb.report2_write.password"))
    println("driver: " + conf.getString("mariadb.report2_write.driver"))
    data.show(10)

//    data
//      .write
//      .mode(SaveMode.Append)
//      .jdbc(mariadb_write_url, tableName, mariadb_write_prop)

  }
}
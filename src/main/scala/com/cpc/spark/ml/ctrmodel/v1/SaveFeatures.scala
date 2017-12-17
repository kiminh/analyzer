package com.cpc.spark.ml.ctrmodel.v1

import java.util.Date

import org.apache.spark.sql.{SaveMode, SparkSession}

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
    saveDataFromLog(spark, date)
  }


  def saveDataFromLog(spark: SparkSession, date: String): Unit = {
    val stmt =
      """
        |select isclick,sex,age,os,isp,network,
        |       city,media_appsid,ext['phone_level'].int_value,`timestamp`,adtype,
        |       planid,unitid,ideaid,ext['adclass'].int_value,adslotid,
        |       adslot_type,ext['pagenum'].int_value,ext['bookid'].string_value
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
}

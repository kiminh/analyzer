package com.cpc.spark.oCPX.basedata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcConversion {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val cv1 = getLabel1(date, hour, spark)
    val cv2 = getLabel2(date, hour, spark)
    val cv3 = getLabel3(date, hour, spark)
    val cv4 = getLabel4(date, hour, spark)

    val result = cv1.union(cv2).union(cv3).union(cv4)
    result
      .repartition(10)
//      .write.mode("overwrite").insertInto("test.ocpc_cvlog_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_cvlog_hourly")
    println("successfully save data into table: dl_cpc.ocpc_cvlog_hourly")
  }

  def getLabel4(date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |select
         |    searchid,
         |    ideaid,
         |    unitid,
         |    userid,
         |    1 as label
         |from
         |     dl_cpc.cpc_conversion
         |where
         |    day='$date'
         |and
         |    `hour` = '$hour'
         |and
         |    (array_contains(conversion_target, 'sdk_site_wz')
         |OR
         |    array_contains(conversion_target, 'js_active_copywx')
         |OR
         |    array_contains(conversion_target, 'sdk_banner_wz')
         |OR
         |    array_contains(conversion_target, 'sdk_popupwindow_wz'))
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("conversion_goal", lit(4))
      .distinct()

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }

  def getLabel3(date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |select
         |    searchid,
         |    ideaid,
         |    unitid,
         |    userid,
         |    1 as label
         |from
         |     dl_cpc.cpc_conversion
         |where
         |    day='$date'
         |and
         |    `hour` = '$hour'
         |and
         |    (array_contains(conversion_target, 'js_active_js_form')
         |OR
         |    array_contains(conversion_target, 'site_form'))
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("conversion_goal", lit(3))
      .distinct()

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }

  def getLabel2(date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |select
         |    searchid,
         |    ideaid,
         |    unitid,
         |    userid,
         |    1 as label
         |from
         |     dl_cpc.cpc_conversion
         |where
         |    day='$date'
         |and
         |    `hour` = '$hour'
         |and
         |    array_contains(conversion_target, 'api')
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("conversion_goal", lit(2))
      .distinct()


    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }

  def getLabel1(date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |select
         |    searchid,
         |    ideaid,
         |    unitid,
         |    userid,
         |    1 as label
         |from
         |     dl_cpc.cpc_conversion
         |where
         |    day='$date'
         |and
         |    `hour` = '$hour'
         |and
         |    array_contains(conversion_target, 'sdk_app_install')
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
        .sql(sqlRequest)
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .withColumn("conversion_goal", lit(1))
        .distinct()

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }
}

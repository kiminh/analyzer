package com.cpc.spark.oCPX.basedata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcConversionV3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val cv1 = getLabel1(date, hour, spark)
    val cv2 = getLabel2(date, hour, spark)
    val cv3 = getLabel3(date, hour, spark)
    val cv4 = getLabel4(date, hour, spark)
    val cv5 = getLabel5(date, hour, spark)
    val cv6 = getLabel6(date, hour, spark)
    val cv7 = getLabel7(date, hour, spark)
    val cv11 = getLabel11(date, hour, spark)
    val cv12 = getLabel12(date, hour, spark)

    val result = cv1
      .union(cv2)
      .union(cv3)
      .union(cv4)
      .union(cv5)
      .union(cv6)
      .union(cv7)
      .union(cv11)
      .union(cv12)
      .distinct()

    result
      .repartition(10)
      .write.mode("overwrite").insertInto("test.ocpc_cvr_log_hourly")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_cvr_log_hourly")
    println("successfully save data into table: dl_cpc.ocpc_cvr_log_hourly")
  }

  def getLabel12(date: String, hour: String, spark: SparkSession) = {
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
         |    array_contains(conversion_target, 'api_ldy_other')
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("conversion_goal", lit(12))
      .withColumn("conversion_from", lit(1))
      .distinct()

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }


  def getLabel11(date: String, hour: String, spark: SparkSession) = {
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
         |    array_contains(conversion_target, 'api_app_other')
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("conversion_goal", lit(11))
      .withColumn("conversion_from", lit(1))
      .distinct()

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }

  def getLabel7(date: String, hour: String, spark: SparkSession) = {
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
         |    array_contains(conversion_target, 'api_app_pay')
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("conversion_goal", lit(7))
      .withColumn("conversion_from", lit(1))
      .distinct()

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }


  def getLabel6(date: String, hour: String, spark: SparkSession) = {
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
         |    array_contains(conversion_target, 'api_app_retention')
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("conversion_goal", lit(6))
      .withColumn("conversion_from", lit(1))
      .distinct()

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }


  def getLabel5(date: String, hour: String, spark: SparkSession) = {
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
         |    array_contains(conversion_target, 'api_app_register')
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("conversion_goal", lit(5))
      .withColumn("conversion_from", lit(1))
      .distinct()

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }


  def getLabel4(date: String, hour: String, spark: SparkSession) = {
    val sqlRequest1 =
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
         |    array_contains(conversion_target, 'js_active_copywx')
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark
      .sql(sqlRequest1)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("conversion_goal", lit(4))
      .withColumn("conversion_from", lit(4))
      .distinct()

    val sqlRequest2 =
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
         |    array_contains(conversion_target, 'sdk_banner_wz')
         |OR
         |    array_contains(conversion_target, 'sdk_popupwindow_wz'))
       """.stripMargin
    println(sqlRequest2)
    val data2 = spark
      .sql(sqlRequest2)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("conversion_goal", lit(4))
      .withColumn("conversion_from", lit(3))
      .distinct()

    val resultDF = data1.union(data2).distinct()

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }

  def getLabel3(date: String, hour: String, spark: SparkSession) = {
    val sqlRequest1 =
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
         |    array_contains(conversion_target, 'js_active_js_form')
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark
      .sql(sqlRequest1)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("conversion_goal", lit(3))
      .withColumn("conversion_from", lit(4))
      .distinct()

    val sqlRequest2 =
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
         |    array_contains(conversion_target, 'site_form')
       """.stripMargin
    println(sqlRequest2)
    val data2 = spark
      .sql(sqlRequest2)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("conversion_goal", lit(3))
      .withColumn("conversion_from", lit(2))
      .distinct()


    val sqlRequest3 =
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
         |    array_contains(conversion_target, 'api_ldy_sform')
       """.stripMargin
    println(sqlRequest3)
    val data3 = spark
      .sql(sqlRequest3)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("conversion_goal", lit(3))
      .withColumn("conversion_from", lit(1))
      .distinct()


    val sqlRequest4 =
      s"""
         |SELECT
         |  t.searchid,
         |  t.ideaid,
         |  t.unitid,
         |  t.userid,
         |  1 as label
         |FROM
         |  (SELECT
         |    searchid,
         |    ideaid,
         |    unitid,
         |    userid,
         |    concat_ws(',', collect_set(conversion_target)) as conversion_targets
         |   FROM
         |    dl_cpc.cpc_conversion
         |   WHERE
         |    day = '$date'
         |   AND
         |    `hour` = '$hour'
         |   AND
         |    array_contains(conversion_target, 'api')) as t
         |WHERE
         |  t.conversion_target not like '%api_%'
         |""".stripMargin
    println(sqlRequest4)
    val data4 = spark
      .sql(sqlRequest4)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("conversion_goal", lit(3))
      .withColumn("conversion_from", lit(1))
      .distinct()

    val resultDF = data1.union(data2).union(data3).union(data4).distinct()

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }

  def getLabel2(date: String, hour: String, spark: SparkSession) = {
    val sqlRequest1 =
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
         |    array_contains(conversion_target, 'api_app_active')
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark
      .sql(sqlRequest1)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("conversion_goal", lit(2))
      .withColumn("conversion_from", lit(1))
      .distinct()

    val sqlRequest2 =
      s"""
         |SELECT
         |  t.searchid,
         |  t.ideaid,
         |  t.unitid,
         |  t.userid,
         |  1 as label
         |FROM
         |  (SELECT
         |    searchid,
         |    ideaid,
         |    unitid,
         |    userid,
         |    concat_ws(',', collect_set(conversion_target)) as conversion_targets
         |   FROM
         |    dl_cpc.cpc_conversion
         |   WHERE
         |    day = '$date'
         |   AND
         |    `hour` = '$hour'
         |   AND
         |    array_contains(conversion_target, 'api')) as t
         |WHERE
         |  t.conversion_target not like '%api_%'
         |""".stripMargin
    println(sqlRequest2)
    val data2 = spark
      .sql(sqlRequest2)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("conversion_goal", lit(2))
      .withColumn("conversion_from", lit(1))
      .distinct()

    val resultDF = data1.union(data2).distinct()


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
        .withColumn("conversion_from", lit(3))
        .distinct()

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }
}

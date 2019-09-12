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

    val result = cv1.union(cv2).union(cv3)
    result
      .repartition(10).write.mode("overwrite").insertInto("test.ocpc_label_cvr_hourly")
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_label_cvr_hourly")
    println("successfully save data into table: dl_cpc.ocpc_unit_label_cvr_hourly")
  }

  def getLabel4(date: String, hour: String, spark: SparkSession) = {
    val sqlRequest1 =
      s"""
         |select
         |    distinct searchid
         |from
         |     dl_cpc.cpc_conversion
         |where
         |    day='$date'
         |and
         |    `hour` = '$hour'
         |and
         |    array_contains(conversion_target, 'sdk_site_wz')
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark.sql(sqlRequest1)

    val sqlRequest2 =
      s"""
         |select
         |    distinct searchid
         |from
         |     dl_cpc.cpc_conversion
         |where
         |    day='$date'
         |and
         |    `hour` = '$hour'
         |and
         |    array_contains(conversion_target, 'js_active_copywx')
       """.stripMargin
    println(sqlRequest2)
    val data2 = spark.sql(sqlRequest2)

    val resultDF = data1
      .union(data2)
      .distinct()
      .select("searchid")
      .withColumn("iscvr", lit(1))
      .withColumn("conversion_goal", lit("cvr4"))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }

  def getLabel3(date: String, hour: String, spark: SparkSession) = {
    val sqlRequest1 =
      s"""
         |select
         |    distinct searchid
         |from
         |     dl_cpc.cpc_conversion
         |where
         |    day='$date'
         |and
         |    `hour` = '$hour'
         |and
         |    array_contains(conversion_target, 'site_form')
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark.sql(sqlRequest1)

    val sqlRequest2 =
      s"""
         |select
         |    distinct searchid
         |from
         |     dl_cpc.cpc_conversion
         |where
         |    day='$date'
         |and
         |    `hour` = '$hour'
         |and
         |    array_contains(conversion_target, 'api')
         |and
         |    (adclass like '134%' or adclass like '107%')
       """.stripMargin
    println(sqlRequest2)
    val data2 = spark.sql(sqlRequest2)

    val sqlRequest3 =
      s"""
         |select
         |    distinct searchid
         |from
         |     dl_cpc.cpc_conversion
         |where
         |    day='$date'
         |and
         |    `hour` = '$hour'
         |and
         |    array_contains(conversion_target, 'js_active_js_form')
       """.stripMargin
    println(sqlRequest3)
    val data3 = spark.sql(sqlRequest3)

    val resultDF = data1
      .union(data2)
      .union(data3)
      .distinct()
      .select("searchid")
      .withColumn("iscvr", lit(1))
      .withColumn("conversion_goal", lit("cvr3"))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }

  def getLabel2(date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |select
         |    distinct searchid
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
      .select("searchid")
      .withColumn("iscvr", lit(1))
      .withColumn("conversion_goal", lit("cvr2"))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }

  def getLabel1(date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |select
         |    distinct searchid
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
        .select("searchid")
        .withColumn("iscvr", lit(1))
        .withColumn("conversion_goal", lit("cvr1"))
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }
}

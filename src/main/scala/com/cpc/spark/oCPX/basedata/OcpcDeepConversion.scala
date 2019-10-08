package com.cpc.spark.oCPX.basedata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcDeepConversion {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val cv1 = getLabel1(date, hour, spark)
    val cv2 = getLabel2(date, hour, spark)
    val cv3 = getLabel3(date, hour, spark)

    val result = cv1.union(cv2).union(cv3).distinct()
    result
//      .repartition(10).write.mode("overwrite").insertInto("test.ocpc_label_deep_cvr_hourly")
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_label_deep_cvr_hourly")
    println("successfully save data into table: dl_cpc.ocpc_label_deep_cvr_hourly")
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
         |    array_contains(conversion_target, 'api_app_retention')
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("deep_conversion_goal", lit(3))

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
         |    array_contains(conversion_target, 'api_app_pay')
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("deep_conversion_goal", lit(2))

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
         |    array_contains(conversion_target, 'api_app_register')
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
        .sql(sqlRequest)
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .withColumn("deep_conversion_goal", lit(1))

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }
}

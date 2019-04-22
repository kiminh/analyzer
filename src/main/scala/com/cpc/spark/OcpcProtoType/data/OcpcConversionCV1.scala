package com.cpc.spark.OcpcProtoType.data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcConversionCV1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val cvrPt = "cvr1test"


    println("parameters:")
    println(s"date=$date, hour=$hour, cvrPt=$cvrPt")
    val result = getLabel(cvrPt, date, hour, spark)
    result
//      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_label_cvr_hourly")
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_label_cvr_hourly")
    println("successfully save data into table: dl_cpc.ocpc_label_cvr_hourly")
  }

  def getLabel(cvrPt: String, date: String, hour: String, spark: SparkSession) = {
    var selectCondition = s"`date`='$date' and hour = '$hour'"

    val sqlRequest =
      s"""
         |select
         |    searchid
         |from dl_cpc.ml_cvr_feature_v1
         |lateral view explode(cvr_list) b as a
         |where $selectCondition
         |and access_channel="sdk"
         |and (a in ("sdk_app_install", "sdk_site_wz"))
         |and adslot_type != 7
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
      .distinct()
      .withColumn("label", lit(1))
      .select("searchid", "label")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("cvr_goal", lit(cvrPt))

    resultDF.show(10)
    resultDF.printSchema()

    resultDF


  }
}

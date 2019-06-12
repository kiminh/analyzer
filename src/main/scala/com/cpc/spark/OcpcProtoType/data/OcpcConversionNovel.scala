package com.cpc.spark.OcpcProtoType.data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcConversionNovel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val conversionGoal = args(2).toInt

    val result = getLabel(conversionGoal, date, hour, spark)
    result
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_label_cvr_hourly")
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_label_cvr_hourly")
    println("successfully save data into table: dl_cpc.ocpc_label_cvr_hourly")
  }

  def getLabel(conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
    var selectCondition = s"`date`='$date' and hour = '$hour'"

    var sqlRequest = ""
    var cvrPt = ""
    if (conversionGoal == 4) {
      // cvr4数据
      sqlRequest =
        s"""
           |select
           |    distinct searchid,1 as label
           |from dl_cpc.ml_cvr_feature_v1
           |lateral view explode(cvr_list) b as a
           |where $selectCondition
           |and access_channel="sdk"
           |and a = "sdk_site_wz"
       """.stripMargin
      cvrPt = "cvr4"
    }
    println(sqlRequest)
    val resultDF = spark
        .sql(sqlRequest)
        .select("searchid", "label")
        .distinct()
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .withColumn("cvr_goal", lit(cvrPt))

    resultDF.show(10)
    resultDF.printSchema()

    resultDF


  }
}

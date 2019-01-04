package com.cpc.spark.ocpcV3.ocpcQtt.data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, sum}

object OcpcLabelCvr4 {
  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val hour = args(1).toString

    val spark = SparkSession.builder().appName(s"ocpc cvr4 label").enableHiveSupport().getOrCreate()
    val result = getLabel(date, hour, spark)
//    result.write.mode("overwrite").saveAsTable("test.ocpcv3_qtt_cvr4_data_hourly")
    result.write.mode("overwrite").insertInto("dl_cpc.ocpcv3_qtt_cvr4_data_hourly")
    println("successfully save data into table: dl_cpc.ocpcv3_qtt_cvr4_data_hourly")
  }

  def getLabel(date: String, hour: String, spark: SparkSession) = {
    var selectWhere = s"`date`='$date' and hour = '$hour'"

    var sqlRequest1 =
      s"""
         |select
         |    searchid,
         |    uid,
         |    ideaid,
         |    unitid,
         |    price,
         |    bid,
         |    userid,
         |    ext['adclass'].int_value as adclass,
         |    isclick,
         |    isshow
         |from dl_cpc.cpc_union_log
         |where $selectWhere
         |and isclick is not null
         |and media_appsid in ("80000001", "80000002")
         |and isshow = 1
         |and ext['antispam'].int_value = 0
         |and ideaid > 0
         |and adsrc = 1
         |and adslot_type in (1,2,3)
      """.stripMargin
    println(sqlRequest1)
    val rawData = spark.sql(sqlRequest1)
    rawData.createOrReplaceTempView("raw_table")

    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label_sdk_dlapp as label
         |FROM
         |  dl_cpc.ml_cvr_feature_v1
         |WHERE
         |  where $selectWhere
         |AND
         |  label_sdk_dlapp=1
         |AND
         |  label_type!=12
       """.stripMargin
    println(sqlRequest2)
    val labelData = spark.sql(sqlRequest2).distinct()

    val resultDF = rawData
      .join(labelData, Seq("searchid"), "left_outer")
      .groupBy("ideaid", "unitid", "adclass")
      .agg(sum(col("label")).alias("cvr4_cnt"))
      .select("ideaid", "unitid", "adclass", "cvr4_cnt")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))


    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }



}
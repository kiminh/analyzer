package com.cpc.spark.ocpcV3.ocpcNovel.data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcLabelCvr2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // TODO 测试
    val result = getLabel(date, hour, spark)
    result.write.mode("overwrite").saveAsTable("test.ocpcv3_base_data_part3")
  }

  def getLabel(date: String, hour: String, spark: SparkSession) = {
    var selectWhere = s"`date`='$date' and hour = '$hour'"

    var sqlRequest1 =
      s"""
         |select
         |    searchid,
         |    uid,
         |    ideaid,
         |    price,
         |    userid,
         |    media_appsid,
         |    ext['adclass'].int_value as adclass,
         |    ext['exp_cvr'].int_value * 1.0 / 1000000 as exp_cvr,
         |    isclick,
         |    isshow
         |from dl_cpc.cpc_union_log
         |where $selectWhere
         |and isclick is not null
         |and media_appsid in ("80001098","80001292","80000001", "80000002")
         |and isshow = 1
         |and ext['antispam'].int_value = 0
         |and ideaid > 0
         |and adsrc = 1
         |and adslot_type in (1,2,3)
         |and ext_int['is_api_callback']!=1
      """.stripMargin
    println(sqlRequest1)
    val rawData = spark.sql(sqlRequest1)
    rawData.createOrReplaceTempView("raw_table")

    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label
         |FROM
         |  dl_cpc.ml_cvr_feature_v2
         |WHERE
         |  where $selectWhere
         |AND
         |  label=1
       """.stripMargin
    println(sqlRequest2)
    val labelData = spark.sql(sqlRequest2)

    val resultDF = rawData
      .join(labelData, Seq("searchid"), "left_outer")
      .groupBy("ideaid", "adclass", "media_appsid")
      .agg(sum(col("label")).alias("cvr2_cnt"))
      .select("ideaid", "adclass", "media_appsid", "cvr2_cnt")

    println("###############complete########")

    resultDF.show(10)
    resultDF.printSchema()

    resultDF


  }
}
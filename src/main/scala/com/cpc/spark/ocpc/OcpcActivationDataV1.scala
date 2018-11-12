package com.cpc.spark.ocpc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcActivationDataV1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OcpcMonitor").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString

    getDataHourlyV1(date, hour, spark)

  }
  def getDataHourly(date: String, hour: String, spark: SparkSession) :Unit = {
    var selectWhere = s"`date`='$date' and hour = '$hour'"
    var sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  ext['adclass'].int_value as adclass,
         |  price,
         |  isclick,
         |  iscvr as isact
         |FROM
         |  dl_cpc.cpc_api_union_log
         |WHERE
         |  $selectWhere
         |AND isclick is not null
         |and media_appsid  in ("80000001", "80000002")
         |and isshow = 1
         |and ext['antispam'].int_value = 0
         |and ideaid > 0
         |and adsrc = 1
         |and adslot_type in (1,2,3)
      """.stripMargin
    println(sqlRequest)
    val base = spark.sql(sqlRequest).filter("isclick=1")

    val resultDF = base
      .groupBy("ideaid", "adclass")
      .agg(
        sum(col("price")).alias("cost"),
        sum(col("isclick")).alias("ctr_cnt"),
        sum(col("isact")).alias("cvr_cnt"))
      .select("ideaid", "adclass", "cost", "ctr_cnt", "cvr_cnt")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    //    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_ideaid_adclass_label3_track")

    resultDF.write.mode("overwrite").insertInto("dl_cpc.ocpc_ideaid_adclass_label3_track_v1")

//    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_ideaid_adclass_label3_track_v1")
  }


  def getDataHourlyV1(date: String, hour: String, spark: SparkSession) :Unit = {
    /**
      * ctr数据：cpc_union_log
      * cvr数据：ml_cvr_feature_v2
      */
    var selectWhere = s"`date`='$date' and hour = '$hour'"
    var sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  ext['adclass'].int_value as adclass,
         |  price,
         |  isclick
         |FROM
         |  dl_cpc.cpc_union_log
         |WHERE
         |  $selectWhere
         |AND isclick is not null
         |and media_appsid  in ("80000001", "80000002")
         |and isshow = 1
         |and ext['antispam'].int_value = 0
         |and ideaid > 0
         |and adsrc = 1
         |and adslot_type in (1,2,3)
      """.stripMargin
    println(sqlRequest)
    val base = spark.sql(sqlRequest)

    val ctrData = base
      .filter("isclick=1")
      .groupBy("ideaid", "adclass")
      .agg(
        sum(col("price")).alias("cost"),
        sum(col("isclick")).alias("ctr_cnt"))
      .select("ideaid", "adclass", "cost", "ctr_cnt")

    val cvrData = spark
      .table("dl_cpc.ml_cvr_feature_v2")
      .where(s"`date`='$date' and `hour`='$hour'")
      .groupBy("ideaid")
      .agg(sum(col("label")).alias("cvr_cnt"))
      .select("ideaid", "cvr_cnt")

    val resultDF = ctrData
      .join(cvrData, Seq("ideaid"), "left_outer")
      .select("ideaid", "adclass", "cost", "ctr_cnt", "cvr_cnt")
      .withColumn("cvr_cnt", when(col("cvr_cnt").isNull, 0).otherwise(col("cvr_cnt")))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))


//    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_ideaid_adclass_label3_track_v1")
    resultDF.write.mode("overwrite").insertInto("dl_cpc.ocpc_ideaid_adclass_label3_track_v1")

  }
}

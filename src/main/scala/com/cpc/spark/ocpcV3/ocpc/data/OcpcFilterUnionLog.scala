package com.cpc.spark.ocpcV3.ocpc.data

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcFilterUnionLog {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val result = getOcpcUnionlog(date, hour, spark)
    result
      .repartition(100).write.mode("overwrite").saveAsTable("test.filtered_union_log_hourly")
//    result
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.filtered_union_log_hourly")
    println("successfully save data into table: dl_cpc.filtered_union_log_hourly")
  }

  def getOcpcUnionlog(date: String, hour: String, spark: SparkSession) = {
    var selectWhere = s"(`date`='$date' and hour = '$hour')"

    // 拿到基础数据
    var sqlRequest =
      s"""
         |select
         |    searchid,
         |    timestamp,
         |    network,
         |    ip,
         |    exptags,
         |    media_type,
         |    media_appsid,
         |    adslotid,
         |    adslot_type,
         |    adnum,
         |    isfill,
         |    adtype,
         |    adsrc,
         |    interaction,
         |    bid,
         |    floorbid,
         |    cpmbid,
         |    price,
         |    ctr,
         |    cpm,
         |    ideaid,
         |    unitid,
         |    planid,
         |    country,
         |    province,
         |    city,
         |    isp,
         |    uid,
         |    ua,
         |    os,
         |    screen_w,
         |    screen_h,
         |    brand,
         |    model,
         |    sex,
         |    age,
         |    coin,
         |    isshow,
         |    show_timestamp,
         |    show_network,
         |    show_ip,
         |    isclick,
         |    click_timestamp,
         |    click_network,
         |    click_ip,
         |    antispam_score,
         |    antispam_rules,
         |    duration,
         |    userid,
         |    interests,
         |    ext,
         |    ext_int,
         |    ext_string,
         |    ext_float
         |from dl_cpc.cpc_union_log
         |where $selectWhere
         |and (isshow>0 or isclick>0)
      """.stripMargin
    println(sqlRequest)
    val rawData = spark
      .sql(sqlRequest)


    val resultDF = rawData
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF.printSchema()

    resultDF
  }

}



package com.cpc.spark.ocpcV3.ocpc.data

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcUnionLog {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val result = getOcpcUnionlog(date, hour, spark)
    result.write.mode("overwrite").saveAsTable("test.ocpc_union_log_hourly")
//    result
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_union_log_hourly")
    println("successfully save data into table: dl_cpc.ocpc_union_log_hourly")
  }

  def getOcpcUnionlog(date: String, hour: String, spark: SparkSession) = {
    var selectWhere = s"`date`='$date' and hour = '$hour'"

    // 拿到基础数据
    var sqlRequest1 =
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
         |    ext_float,
         |    ext_string['ocpc_log'] as ocpc_log
         |from dl_cpc.cpc_union_log
         |where $selectWhere
         |and isclick is not null
         |and isshow = 1
         |and ext['antispam'].int_value = 0
         |and ideaid > 0
         |and adsrc = 1
      """.stripMargin
    println(sqlRequest1)
    val rawData = spark
      .sql(sqlRequest1)
      .withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))
      .filter(s"length(ocpc_log)>0")


    val resultDF = rawData
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF.printSchema()

    resultDF
  }

}


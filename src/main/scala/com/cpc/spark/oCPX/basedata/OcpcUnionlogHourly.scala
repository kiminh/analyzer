package com.cpc.spark.oCPX.basedata

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcUnionlogHourly {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val data = getBaseUnionlog(date, hour, spark)

    data
      .repartition(100)
//      .write.mode("overwrite").insertInto("test.ocpc_base_unionlog_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_base_unionlog_hourly")

    println("successfully save data into table: dl_cpc.ocpc_base_unionlog_hourly")


    val ocpcData = getOcpcUnionlog(data, date, hour, spark)
    ocpcData
      .repartition(50)
//      .write.mode("overwrite").insertInto("test.ocpc_filter_unionlog_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_filter_unionlog_hourly")

    println("successfully save data into table: dl_cpc.ocpc_filter_unionlog_hourly")
  }

  def getOcpcUnionlog(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val baseData = data
        .filter(s"ocpc_step = 2")
        .withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))
        .withColumn("deep_ocpc_log_dict", udfStringToMap()(col("deep_ocpc_log")))

    baseData.createOrReplaceTempView("base_data")

    val sqlRequest =
      s"""
         |select
         |    searchid,
         |    exptags,
         |    media_type,
         |    media_appsid,
         |    adslotid,
         |    adslot_type,
         |    adtype,
         |    bid,
         |    price,
         |    ideaid,
         |    unitid,
         |    planid,
         |    country,
         |    province,
         |    city,
         |    uid,
         |    os,
         |    isshow,
         |    isclick,
         |    duration,
         |    userid,
         |    is_ocpc,
         |    ocpc_log_dict,
         |    user_city,
         |    city_level,
         |    adclass,
         |    exp_ctr,
         |    exp_cvr,
         |    raw_cvr,
         |    usertype,
         |    conversion_goal,
         |    conversion_from,
         |    is_api_callback,
         |    cvr_model_name,
         |    bid_discounted_by_ad_slot,
         |    bscvr,
         |    ocpc_expand,
         |    deep_cvr,
         |    raw_deep_cvr,
         |    deep_cvr_model_name,
         |    deep_ocpc_log_dict,
         |    is_deep_ocpc,
         |    deep_conversion_goal,
         |    deep_cpa,
         |    cpa_check_priority
         |from
         |    base_data
       """.stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)

    val resultDF = rawData
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF.printSchema()

    resultDF

  }

  def getBaseUnionlog(date: String, hour: String, spark: SparkSession) = {
    val selectWhere = s"(`day`='$date' and hour = '$hour')"
    // 新版基础数据抽取逻辑
    val sqlRequest =
      s"""
         |select
         |    searchid,
         |    concat_ws(',', exptags) as exptags,
         |    media_type,
         |    media_appsid,
         |    adslot_id as adslotid,
         |    adslot_type,
         |    adtype,
         |    bid,
         |    price,
         |    ideaid,
         |    unitid,
         |    planid,
         |    country,
         |    province,
         |    city,
         |    uid,
         |    os,
         |    isshow,
         |    isclick,
         |    0 as duration,
         |    userid,
         |    cast(is_ocpc as int) as is_ocpc,
         |    (case when isclick=1 then ocpc_log else '' end) as ocpc_log,
         |    user_city,
         |    city_level,
         |    adclass,
         |    cast(exp_ctr * 1.0 / 1000000 as double) as exp_ctr,
         |    cast(exp_cvr * 1.0 / 1000000 as double) as exp_cvr,
         |    raw_cvr,
         |    usertype,
         |    conversion_goal,
         |    conversion_from,
         |    is_api_callback,
         |    cvr_model_name,
         |    bid_discounted_by_ad_slot,
         |    ocpc_step,
         |    ocpc_status,
         |    bscvr,
         |    ocpc_expand,
         |    ext_string['exp_ids'] as expids,
         |    deep_cvr,
         |    raw_deep_cvr,
         |    deep_cvr_model_name,
         |    deep_ocpc_log,
         |    is_deep_ocpc,
         |    deep_conversion_goal,
         |    deep_cpa,
         |    cpa_check_priority
         |from dl_cpc.cpc_basedata_union_events
         |where $selectWhere
         |and (isshow>0 or isclick>0)
         |and adslot_type != 7
         |and length(searchid) > 0
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




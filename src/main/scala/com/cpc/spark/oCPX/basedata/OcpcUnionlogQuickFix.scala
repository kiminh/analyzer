package com.cpc.spark.oCPX.basedata

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcUnionlogQuickFix {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val data = getBaseUnionlog(date, hour, spark)

    data
      .repartition(100)
//      .write.mode("overwrite").insertInto("test.ocpc_base_unionlog")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_base_unionlog")

    println("successfully save data into table: dl_cpc.ocpc_base_unionlog")


    val ocpcData = getOcpcUnionlog(data, date, hour, spark)
    ocpcData
      .repartition(50)
//      .write.mode("overwrite").insertInto("test.ocpc_filter_unionlog")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_filter_unionlog")

    println("successfully save data into table: dl_cpc.ocpc_filter_unionlog")
  }

  def getOcpcUnionlog(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    // DONE 调整过滤条件：ocpc_Step
    val baseData = data
        .filter(s"ocpc_step = 2")
        .withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))
        .withColumn("deep_ocpc_log_dict", udfStringToMap()(col("deep_ocpc_log")))

    baseData.createOrReplaceTempView("base_data")

    val sqlRequest =
      s"""
         |select
         |    searchid,
         |    timestamp,
         |    network,
         |    exptags,
         |    media_type,
         |    media_appsid,
         |    adslotid,
         |    adslot_type,
         |    adtype,
         |    adsrc,
         |    interaction,
         |    bid,
         |    price,
         |    ideaid,
         |    unitid,
         |    planid,
         |    country,
         |    province,
         |    city,
         |    uid,
         |    ua,
         |    os,
         |    sex,
         |    age,
         |    isshow,
         |    isclick,
         |    duration,
         |    userid,
         |    is_ocpc,
         |    user_city,
         |    city_level,
         |    adclass,
         |    ocpc_log_dict,
         |    exp_ctr,
         |    exp_cvr,
         |    antispam,
         |    conversion_goal,
         |    charge_type,
         |    conversion_from,
         |    is_api_callback,
         |    siteid,
         |    cvr_model_name,
         |    user_req_ad_num,
         |    user_req_num,
         |    is_new_ad,
         |    is_auto_coin,
         |    bid_discounted_by_ad_slot,
         |    second_cpm,
         |    final_cpm,
         |    exp_cpm,
         |    ocpc_expand,
         |    expids,
         |    bsctr,
         |    bscvr,
         |    raw_cvr,
         |    deep_cvr,
         |    raw_deep_cvr,
         |    deep_cvr_model_name,
         |    deep_ocpc_log_dict,
         |    is_deep_ocpc,
         |    deep_conversion_goal,
         |    deep_cpa,
         |    cpa_check_priority,
         |    ocpc_expand_tag
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
    spark.udf.register("getConversionGoal", (traceType: String, traceOp1: String, traceOp2: String) => {
      var result = -1
      if (traceOp1 == "REPORT_DOWNLOAD_PKGADDED") {
        result = 1
      } else if (traceType == "active_third" && traceOp2 == "") {
        result = 0
      } else if (traceType == "active_third" && traceOp2 == "0") {
        result = 2
      } else if (traceType == "active_third" && traceOp2 == "1") {
        result = 5
      } else if (traceType == "active_third" && traceOp2 == "2") {
        result = 7
      } else if (traceType == "active_third" && traceOp2 == "5") {
        result = 11
      } else if (traceType == "active_third" && traceOp2 == "6") {
        result = 6
      } else if (traceType == "active_third" && traceOp2 == "26") {
        result = 3
      } else if (traceType == "active_third" && traceOp2 == "27") {
        result = 12
      } else if (traceType == "active15" && traceOp2 == "site_form") {
        result = 3
      } else if (traceType == "ctsite_active15" && traceOp2 == "ct_site_form") {
        result = 3
      } else if (traceType == "js_active" && traceOp2 == "js_form") {
        result = 3
      } else if (traceOp1 == "REPORT_USER_STAYINWX") {
        result = 4
      } else if (traceType == "js_active" && traceOp2 == "active_copywx") {
        result = 4
      } else if (traceOp1 == "REPORT_ICON_STAYINWX" && traceOp2 == "ON_BANNER") {
        result = 4
      } else if (traceOp1 == "REPORT_ICON_STAYINWX" && traceOp2 == "CLICK_POPUPWINDOW_ADDWX") {
        result = 4
      } else {
        result = -1
      }
      result
    })

    var selectWhere = s"(`day`='$date' and hour = '$hour')"
    // 新版基础数据抽取逻辑
    // done 调整ocpc_log的存在逻辑
    var sqlRequest =
      s"""
         |select
         |    searchid,
         |    timestamp,
         |    network,
         |    concat_ws(',', exptags) as exptags,
         |    media_type,
         |    media_appsid,
         |    adslot_id as adslotid,
         |    adslot_type,
         |    adtype,
         |    adsrc,
         |    interaction,
         |    bid,
         |    price,
         |    ideaid,
         |    unitid,
         |    planid,
         |    country,
         |    province,
         |    city,
         |    uid,
         |    ua,
         |    os,
         |    sex,
         |    age,
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
         |    charge_type,
         |    0 as antispam,
         |    usertype,
         |    conversion_goal,
         |    conversion_from,
         |    is_api_callback,
         |    siteid,
         |    cvr_model_name,
         |    user_req_ad_num,
         |    user_req_num,
         |    is_new_ad,
         |    is_auto_coin,
         |    bid_discounted_by_ad_slot,
         |    discount,
         |    exp_cpm,
         |    cvr_threshold,
         |    dsp_cpm,
         |    new_user_days,
         |    ocpc_step,
         |    previous_id,
         |    ocpc_status,
         |    bscvr,
         |    second_cpm,
         |    final_cpm,
         |    ocpc_expand,
         |    ext_string['exp_ids'] as expids,
         |    bsctr,
         |    raw_cvr,
         |    deep_cvr,
         |    raw_deep_cvr,
         |    deep_cvr_model_name,
         |    deep_ocpc_log,
         |    is_deep_ocpc,
         |    deep_conversion_goal,
         |    deep_cpa,
         |    cpa_check_priority,
         |    ocpc_expand_tag,
         |    ori_cvr,
         |    uid_mc_show0,
         |    uid_mc_click0
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




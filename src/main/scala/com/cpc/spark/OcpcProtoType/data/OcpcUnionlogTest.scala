package com.cpc.spark.OcpcProtoType.data

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OcpcUnionlogTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val data = getBaseUnionlog(date, hour, spark)

    data
//      .repartition(100).write.mode("overwrite").insertInto("dl_cpc.ocpc_base_unionlog_new")
      .repartition(100).write.mode("overwrite").saveAsTable("test.ocpc_base_unionlog_new")

    println("successfully save data into table: dl_cpc.ocpc_base_unionlog_new")
//
//    val ocpcData = getOcpcUnionlog(data, date, hour, spark)
//    ocpcData
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_filter_unionlog_new")
////      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_filter_unionlog_new")
//    println("successfully save data into table: dl_cpc.ocpc_filter_unionlog_new")
  }

  def getOcpcUnionlog(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val baseData = data
      .filter(s"length(ocpc_log)>0")
      .withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))

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
         |    antispam
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
    var selectWhere = s"(`day`='$date' and hour = '$hour')"

    // 拿到基础数据
    var sqlRequest1 =
      s"""
         |select
         |    searchid,
         |    timestamp,
         |    network,
         |    exptags,
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
         |    ocpc_log as ocpc_log_old,
         |    user_city,
         |    city_level,
         |    adclass,
         |    cast(exp_ctr * 1.0 / 1000000 as double) as exp_ctr,
         |    cast(exp_cvr * 1.0 / 1000000 as double) as exp_cvr,
         |    charge_type,
         |    0 as antispam
         |from dl_cpc.cpc_basedata_union_events
         |where $selectWhere
         |and (isshow>0 or isclick>0)
         |and length(searchid) > 0
      """.stripMargin
    println(sqlRequest1)
    val rawData = spark.sql(sqlRequest1)
    rawData.createOrReplaceTempView("raw_data")

    // 更新ocpc_log
    val augTableName = "dl_cpc.cpc_basedata_as_event"
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  ocpc_log as ocpc_log_new
         |FROM
         |  $augTableName
         |WHERE
         |  $selectWhere
         |AND
         |  length(ocpc_log) > 0
       """.stripMargin
    println(sqlRequest2)
    val augData = spark.sql(sqlRequest2)
    augData.createOrReplaceTempView("aug_data")

    val sqlRequest3 =
      s"""
         |select
         |    a.searchid,
         |    a.timestamp,
         |    a.network,
         |    a.exptags,
         |    a.media_type,
         |    a.media_appsid,
         |    a.adslotid,
         |    a.adslot_type,
         |    a.adtype,
         |    a.adsrc,
         |    a.interaction,
         |    a.bid,
         |    a.price,
         |    a.ideaid,
         |    a.unitid,
         |    a.planid,
         |    a.country,
         |    a.province,
         |    a.city,
         |    a.uid,
         |    a.ua,
         |    a.os,
         |    a.sex,
         |    a.age,
         |    a.isshow,
         |    a.isclick,
         |    a.duration,
         |    a.userid,
         |    a.is_ocpc,
         |    (case when b.ocpc_log_new is null then a.ocpc_log_old
         |          else b.ocpc_log_new end) as ocpc_log,
         |    a.user_city,
         |    a.city_level,
         |    a.adclass,
         |    a.exp_ctr,
         |    a.exp_cvr,
         |    a.charge_type,
         |    a.antispam,
         |    a.ocpc_log_old,
         |    b.ocpc_log_new
         |from raw_data as a
         |left join aug_data as b
         |ON
         |    a.searchid = b.searchid
       """.stripMargin
    println(sqlRequest3)
    val data = spark.sql(sqlRequest3)

    val resultDF = data
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF.printSchema()

    resultDF
  }



}





package com.cpc.spark.ocpcV3.ocpc.data

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OcpcFilterUnionLog {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val data = getUnionlog(date, hour, spark)

    data
      .repartition(100).write.mode("overwrite").insertInto("dl_cpc.filtered_union_log_hourly")
    println("successfully save data into table: dl_cpc.filtered_union_log_hourly")

    // 按需求增加需要进行抽取的数据表
    // bid
    val bidData = getBidUnionlog(data, date, hour, spark)
    bidData
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .repartition(50).write.mode("overwrite").insertInto("dl_cpc.filtered_union_log_bid_hourly")

    // 增加可供ab对比实验的数据表
    val abData = getAbUnionlog(data, date, hour, spark)
    abData
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .repartition(50).write.mode("overwrite").insertInto("dl_cpc.filtered_union_log_exptag_hourly")
  }

  def getAbUnionlog(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    var selectWhere = s"(`date`='$date' and hour = '$hour')"

    // 拿到基础数据
    var sqlRequest =
      s"""
         |SELECT
         |    searchid,
         |    ideaid,
         |    unitid,
         |    planid,
         |    userid,
         |    uid,
         |    adslotid,
         |    adslot_type,
         |    adtype,
         |    adsrc,
         |    exptags,
         |    media_type,
         |    media_appsid,
         |    ext['adclass'].int_value as adclass,
         |    ext_string['ocpc_log'] as ocpc_log,
         |    cast(ext['exp_ctr'].int_value * 1.0 / 1000000 as double) as exp_ctr,
         |    cast(ext['exp_cvr'].int_value * 1.0 / 1000000 as double) as exp_cvr,
         |    bid as original_bid,
         |    price,
         |    isshow,
         |    isclick
         |FROM
         |    dl_cpc.cpc_union_log
         |WHERE
         |    $selectWhere
         |and (isshow>0 or isclick>0)
      """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest).withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))
    resultDF
  }

  def getBidUnionlog(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val data = rawData.withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))

    data.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  ideaid,
         |  unitid,
         |  planid,
         |  userid,
         |  uid,
         |  adslotid,
         |  adslot_type,
         |  adtype,
         |  adsrc,
         |  exptags,
         |  media_type,
         |  media_appsid,
         |  bid as original_bid,
         |  (case when length(ocpc_log)>0 then cast(ocpc_log_dict['dynamicbid'] as int)
         |        else bid end) as bid,
         |  ocpc_log
         |FROM
         |  base_data
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)

    resultDF
  }


  def getUnionlog(date: String, hour: String, spark: SparkSession) = {
    var selectWhere = s"(`date`='$date' and hour = '$hour')"

    // 拿到基础数据
    var sqlRequest =
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
         |    ext_int['is_ocpc'] as is_ocpc,
         |    ext_string['ocpc_log'] as ocpc_log,
         |    ext_string['user_city'] as user_city,
         |    ext['city_level'].int_value as city_level,
         |    ext['adclass'].int_value as adclass
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



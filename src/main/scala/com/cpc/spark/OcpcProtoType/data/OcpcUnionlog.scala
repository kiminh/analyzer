package com.cpc.spark.OcpcProtoType.data

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OcpcUnionlog {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val data = getBaseUnionlog(date, hour, spark)

    data
      .repartition(100).write.mode("overwrite").insertInto("dl_cpc.ocpc_base_unionlog")
//      .repartition(100).write.mode("overwrite").saveAsTable("test.ocpc_base_unionlog")

    println("successfully save data into table: dl_cpc.ocpc_base_unionlog")

    val ocpcData = getOcpcUnionlog(data, date, hour, spark)
    ocpcData
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_filter_unionlog")
//      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_filter_unionlog")
    println("successfully save data into table: dl_cpc.ocpc_filter_unionlog")
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
         |    antispam,
         |    conversion_goal,
         |    charge_type
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

//    // 旧版基础数据抽取逻辑
//    // 拿到基础数据
//    var sqlRequest =
//      s"""
//         |select
//         |    searchid,
//         |    timestamp,
//         |    network,
//         |    exptags,
//         |    media_type,
//         |    media_appsid,
//         |    adslotid,
//         |    adslot_type,
//         |    adtype,
//         |    adsrc,
//         |    interaction,
//         |    bid,
//         |    price,
//         |    ideaid,
//         |    unitid,
//         |    planid,
//         |    country,
//         |    province,
//         |    city,
//         |    uid,
//         |    ua,
//         |    os,
//         |    sex,
//         |    age,
//         |    isshow,
//         |    isclick,
//         |    duration,
//         |    userid,
//         |    ext_int['is_ocpc'] as is_ocpc,
//         |    ext_string['ocpc_log'] as ocpc_log,
//         |    ext_string['user_city'] as user_city,
//         |    ext['city_level'].int_value as city_level,
//         |    ext['adclass'].int_value as adclass,
//         |    cast(ext['exp_ctr'].int_value * 1.0 / 1000000 as double) as exp_ctr,
//         |    cast(ext['exp_cvr'].int_value * 1.0 / 1000000 as double) as exp_cvr,
//         |    cast(ext["charge_type"].int_value as int) as charge_type,
//         |    ext['antispam'].int_value as antispam,
//         |    cast(ext['usertype'].int_value as bigint) as usertype
//         |from dl_cpc.cpc_union_log
//         |where $selectWhere
//         |and (isshow>0 or isclick>0)
//      """.stripMargin
//    println(sqlRequest)
//    val rawData = spark
//      .sql(sqlRequest)

    // 新版基础数据抽取逻辑
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
         |    ocpc_log,
         |    user_city,
         |    city_level,
         |    adclass,
         |    cast(exp_ctr * 1.0 / 1000000 as double) as exp_ctr,
         |    cast(exp_cvr * 1.0 / 1000000 as double) as exp_cvr,
         |    charge_type,
         |    (case when antispam_score < 10000 and isclick=1 then 1 else 0 end) as antispam,
         |    usertype,
         |    conversion_goal
         |from dl_cpc.cpc_basedata_union_events
         |where $selectWhere
         |and (isshow>0 or isclick>0)
         |and adslot_type != 7
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




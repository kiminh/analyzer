package com.cpc.spark.ocpcV3.ocpcNovel.data

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcUnionlogNovel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val result = getOcpcUnionlog(date, hour, spark)
    result.write.mode("overwrite").insertInto("dl_cpc.ocpcv3_unionlog_label_hourly")
    println("successfully save data into table: dl_cpc.ocpcv3_unionlog_label_hourly")
  }

  def getOcpcUnionlog(date: String, hour: String, spark: SparkSession) = {
    var selectWhere = s"`date`='$date' and hour = '$hour'"

    // 拿到基础数据
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
         |    media_appsid,
         |    ext['adclass'].int_value as adclass,
         |    ext['exp_cvr'].int_value * 1.0 / 1000000 as exp_cvr,
         |    isclick,
         |    isshow,
         |    exptags,
         |    ext_int['is_api_callback'] as is_api_callback,
         |    ext_int['bid_ocpc'] as cpa_given,
         |    ext_string['ocpc_log'] as ocpc_log
         |from dl_cpc.cpc_union_log
         |where $selectWhere
         |and isclick is not null
         |and media_appsid in ("80001098","80001292")
         |and isshow = 1
         |and ext['antispam'].int_value = 0
         |and ideaid > 0
         |and adsrc = 1
         |and adslot_type in (1,2,3)
      """.stripMargin
    println(sqlRequest1)
    val rawData = spark
      .sql(sqlRequest1).withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))
      .filter(s"length(ocpc_log)>0")

    // cvr1：安装类
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  1 as iscvr1
         |FROM
         |  dl_cpc.ml_cvr_feature_v1
         |WHERE
         |  where $selectWhere
         |AND
         |  label2=1
         |AND
         |  label_type!=12
       """.stripMargin
    println(sqlRequest2)
    val labelData1 = spark.sql(sqlRequest2).distinct()

    // cvr2: api回传类
    val sqlRequest3 =
      s"""
         |SELECT
         |  searchid,
         |  1 as iscvr2
         |FROM
         |  dl_cpc.ml_cvr_feature_v2
         |WHERE
         |  where $selectWhere
         |AND
         |  label=1
       """.stripMargin
    println(sqlRequest3)
    val labelData2 = spark.sql(sqlRequest3).distinct()

    // 表单类
    val sqlRequest4 =
      s"""
         |SELECT
         |  searchid,
         |  1 as iscvr3
         |FROM
         |  dl_cpc.ml_cvr_feature_v1
         |WHERE
         |  where $selectWhere
         |AND
         |  label2=1
         |AND
         |  label_type!=12
       """.stripMargin
    println(sqlRequest4)
    val labelData3 = spark.sql(sqlRequest4).distinct()

    val resultDF = rawData
      .join(labelData1, Seq("searchid"), "left_outer")
      .join(labelData2, Seq("searchid"), "left_outer")
      .join(labelData3, Seq("searchid"), "left_outer")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }

}

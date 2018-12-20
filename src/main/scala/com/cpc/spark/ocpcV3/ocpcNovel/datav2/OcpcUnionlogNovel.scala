package com.cpc.spark.ocpcV3.ocpcNovel.datav2

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
    val tableName = "test.ocpcv3_unionlog_label_hourly_v2"
    result.write.mode("overwrite").saveAsTable(tableName)
//    result.write.mode("overwrite").insertInto(tableName)
    println(s"successfully save data into table: $tableName")
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
         |    adslotid,
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
       """.stripMargin
    println(sqlRequest2)
    val labelData = spark.sql(sqlRequest2).distinct()

    val resultDF = rawData
      .join(labelData, Seq("searchid"), "left_outer")
      .select("searchid", "uid", "ideaid", "unitid", "adslotid", "price", "bid", "userid", "media_appsid", "adclass", "exp_cvr", "isclick", "isshow", "exptags string", "is_api_callback", "cpa_given", "ocpc_log", "ocpc_log_dict", "iscvr1")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }

}

package com.cpc.spark.ocpcV3.ocpcNovel.data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcMotivationTable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // TODO 测试
    val result = filterTable(date, hour, spark)
    result.write.mode("overwrite").saveAsTable("test.ocpcv3_motivation_unionlog_hourly")
    println("successfully save table into dl_cpc.ocpcv3_motivation_unionlog_hourly")
  }

  def filterTable(date: String, hour: String, spark: SparkSession) = {
    var selectWhere = s"`date`='$date' and hour = '$hour'"

    // unionlog数据
    var sqlRequest1 =
      s"""
         |select
         |    searchid,
         |    ideaid,
         |    unitid,
         |    userid,
         |    media_appsid,
         |    ext['adclass'].int_value as adclass
         |from dl_cpc.cpc_union_log
         |where $selectWhere
         |and isclick is not null
         |and media_appsid in ("80001098","80001292","80000001", "80000002")
         |and isshow = 1
         |and ext['antispam'].int_value = 0
         |and ideaid > 0
         |and adsrc = 1
         |and adslot_type in (1,2,3)
         |and ext_int['is_api_callback']=1
      """.stripMargin
    println(sqlRequest1)
    val unionLog = spark.sql(sqlRequest1)
    unionLog.show(10)
    unionLog.write.mode("overwrite").saveAsTable("test.test_ocpcv3_motivation_unionlog_hourly_unionlog")

    // cpc_motivation数据
    val sqlRequest2 =
      s"""
         |select
         |    searchid,
         |    ideaid,
         |    unitid,
         |    adclass,
         |    userid,
         |    isclick,
         |    isshow
         |from dl_cpc.cpc_motivation_log
         |where $selectWhere
       """.stripMargin
    println(sqlRequest2)
    val motivationLog = spark.sql(sqlRequest2)
    motivationLog.show(10)
    motivationLog.write.mode("overwrite").saveAsTable("test.test_ocpcv3_motivation_unionlog_hourly_motivation")

    // 数据关联，使用unionlog过滤
    val resultDF = unionLog
      .join(motivationLog, Seq("searchid", "ideaid", "unitid", "adclass", "userid"))
      .select("searchid", "ideaid", "unitid", "adclass", "userid", "media_appsid", "isclick", "isshow")

    resultDF.show(10)
    resultDF
  }
}

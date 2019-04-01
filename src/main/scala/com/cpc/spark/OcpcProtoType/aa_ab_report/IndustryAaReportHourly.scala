package com.cpc.spark.OcpcProtoType.aa_ab_report

import org.apache.spark.sql.{DataFrame, SparkSession}

object IndustryAaReportHourly {
  def main(args: Array[String]): Unit = {

  }

  // 首先统计每天的分行业的user数和unit数
  def getUserUnitNum(date: String, hour: String, spark: SparkSession): DataFrame ={
    val sql =
      s"""
        |select
        |    industry,
        |    count(userid) as ocpc_user_num,
        |    count(unitid) as ocpc_unit_num
        |from(select
        |        industry,
        |        userid,
        |        unitid
        |    from
        |        dl_cpc.ocpc_aa_ab_report_base_data
        |    where
        |        `date` = '$date'
        |    and
        |        hour = '$hour'
        |    and
        |        version = 'qtt_demo'
        |    group by
        |        industry,
        |        userid,
        |        unitid) temp
      """.stripMargin
    val userUnitDF = spark.sql(sql)
    userUnitDF
  }
}

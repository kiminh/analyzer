package com.cpc.spark.ocpc

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcHourlyReportV2 {
  def main(args: Array[String]): Unit = {
    /*
    新版报表程序
    1. 从ocpc_unionlog拉取ocpc广告记录
    2. 采用数据关联方式获取转化数据
    3. 统计相关数据
     */
    val spark = SparkSession
      .builder()
      .appName("OcpcHourlyReport")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString

    // 拉取点击、消费、转化等基础数据
    val rawData = getBaseData(date, hour, spark)

    // 分ideaid和conversion_goal统计转化成本
//    val data = preprocessData(rawData, date, hour, spark)

    // 输出结果表
//    val result = saveDataToHdfs(data, date, hour, spark)
  }

  def getBaseData(date: String, hour: String, spark: SparkSession) = {
    /**
      * 重新计算抽取全天截止当前时间的数据日志
      */

    // 抽取基础数据：所有跑ocpc的广告主
    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  ideaid,
         |  userid,
         |  isclick,
         |  isshow,
         |  price,
         |  cast(ocpc_log_dict['cpagiven'] as double) as cpagiven,
         |  cast(ocpc_log_dict['dynamicbid'] as double) as bid,
         |  cast(ocpc_log_dict['kvalue'] as double) as kvalue,
         |  cast(ocpc_log_dict['conversiongoal'] as int) as conversiongoal,
         |  cast(ocpc_log_dict['ocpcstep'] as int) as ocpcstep
         |FROM
         |  dl_cpc.ocpc_unionlog
         |WHERE
         |  `dt`='$date' and `hour` <= '$hour'
       """.stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)


    // 关联转化表
    val selectCondition = s"`date`='$date'"
    // cvr1
    val cvr1Data = spark
      .table("dl_cpc.ml_cvr_feature_v1")
      .where(selectCondition)
      .filter(s"label2=1")
      .select("searchid")
      .withColumn("iscvr1", lit(1))
      .distinct()

    // cvr2
    val cvr2Data = spark
      .table("dl_cpc.ml_cvr_feature_v2")
      .where(selectCondition)
      .filter(s"label=1")
      .select("searchid")
      .withColumn("iscvr2", lit(1))
      .distinct()

    // cvr3
    val cvr3Data = spark
      .table("dl_cpc.site_form_unionlog")
      .where(selectCondition)
      .select("searchid")
      .withColumn("iscvr3", lit(1))
      .distinct()

    // 数据关联
    val resultDF = rawData
      .join(cvr1Data, Seq("searchid"), "left_outer")
      .join(cvr2Data, Seq("searchid"), "left_outer")
      .join(cvr3Data, Seq("searchid"), "left_outer")
      .select("searchid", "ideaid", "userid", "isclick", "isshow", "price", "cpagiven", "bid", "kvalue", "conversiongoal", "ocpcstep", "iscvr1", "iscvr2", "iscvr3")

    resultDF.show(10)

    resultDF

  }


}
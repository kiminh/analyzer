package com.cpc.spark.OcpcProtoType.report_novel

//import com.cpc.spark.tools.testOperateMySQL
//import com.typesafe.config.ConfigFactory
//import org.apache.spark.sql.functions._
import com.cpc.spark.OcpcProtoType.report.OcpcHourlyReport._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcHourlyReportV3 {
  def main(args: Array[String]): Unit = {
    /*
    新版报表程序
    1. 从ocpc_unionlog拉取ocpc广告记录
    2. 采用数据关联方式获取转化数据
    3. 统计分ideaid级别相关数据
    4. 统计分conversion_goal级别相关数据
    5. 存储到hdfs
    6. 存储到mysql
     */
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .appName("OcpcHourlyReport")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, media=$media")

    // 拉取点击、消费、转化等基础数据
    var isHidden = 0
    if (version == "novel_v3") {
      isHidden = 0
    } else {
      isHidden = 1
    }
    val baseData = getBaseData2(media, date, hour, spark).filter(s"is_hidden = $isHidden")

    // 分unitid和conversion_goal统计数据
    val rawDataUnit = preprocessDataByUnit(baseData, date, hour, spark)
    val dataUnit = getDataByUnit(rawDataUnit, version, date, hour, spark)

    // 分userid和conversion_goal统计数据
    val rawDataUser = preprocessDataByUser(baseData, date, hour, spark)
    val dataUser = getDataByUser(rawDataUser, version, date, hour, spark)


    // 分conversion_goal统计数据
    val rawDataConversion = preprocessDataByConversion(dataUnit, date, hour, spark)
    val costDataConversion = preprocessCostByConversion(dataUnit, date, hour, spark)
    val cpaDataConversion = preprocessCpaByConversion(baseData, date, hour, spark)
    val dataConversion = getDataByConversionV2(rawDataConversion, version, costDataConversion, cpaDataConversion, date, hour, spark)

    // 存储数据到hadoop
    saveDataToHDFS(dataUnit, dataUser, dataConversion, version, date, hour, spark)
  }

  def saveDataToHDFS(dataUnit: DataFrame, dataUser: DataFrame, dataConversion: DataFrame, version: String, date: String, hour: String, spark: SparkSession) = {
    /*
    存储unitid级别和conversion_goal级别的报表到hdfs
     */
    val versionUnit = version + "_unitid"
    dataUnit
      .withColumn("identifier", col("unitid"))
      .selectExpr("cast(identifier as string) identifier", "userid", "conversion_goal", "step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k", "pre_cvr", "post_cvr", "q_factor", "acb", "auc", "date", "hour")
      .withColumn("version", lit(versionUnit))
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_detail_report_hourly_v4")

    val versionUser = version + "_userid"
    dataUser
      .withColumn("identifier", col("userid"))
      .selectExpr("cast(identifier as string) identifier", "userid", "conversion_goal", "step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k", "pre_cvr", "post_cvr", "q_factor", "acb", "auc", "date", "hour")
      .withColumn("version", lit(versionUser))
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_detail_report_hourly_v4")


    dataUnit
      .withColumn("identifier", col("unitid"))
      .selectExpr("cast(identifier as string) identifier", "userid", "conversion_goal", "cali_value", "cali_pcvr", "cali_postcvr", "smooth_factor", "cpa_suggest", "hourly_expcvr", "hourly_calivalue", "hourly_calipcvr", "hourly_calipostcvr", "date", "hour")
      .withColumn("version", lit(versionUnit))
//      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_cali_detail_report_hourly_20190413")
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_cali_detail_report_hourly")


    dataUser
      .withColumn("identifier", col("userid"))
      .selectExpr("cast(identifier as string) identifier", "userid", "conversion_goal", "cali_value", "cali_pcvr", "cali_postcvr", "smooth_factor", "cpa_suggest", "hourly_expcvr", "hourly_calivalue", "hourly_calipcvr", "hourly_calipostcvr", "date", "hour")
      .withColumn("version", lit(versionUser))
//      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_cali_detail_report_hourly_20190413_user")
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_cali_detail_report_hourly")


    dataConversion
      .selectExpr("conversion_goal", "total_adnum", "step2_adnum", "low_cpa_adnum", "high_cpa_adnum", "step2_cost", "step2_cpa_high_cost", "impression", "click", "conversion", "ctr", "click_cvr", "cost", "acp", "pre_cvr", "post_cvr", "q_factor", "acb", "auc", "cpa_given", "cpa_real", "cpa_ratio", "date", "hour")
      .withColumn("version", lit(version))
//      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_summary_report_hourly_v3_20190413")
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_summary_report_hourly_v4")

  }

  def getBaseData2(media: String, date: String, hour: String, spark: SparkSession) = {
    /**
      * 重新计算抽取全天截止当前时间的数据日志
      */
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 抽取基础数据：所有跑ocpc的广告主
    // todo
    val sqlRequest =
    s"""
       |SELECT
       |    searchid,
       |    unitid,
       |    userid,
       |    isclick,
       |    isshow,
       |    price,
       |    exp_cvr,
       |    exp_ctr,
       |    cast(ocpc_log_dict['cpagiven'] as double) as cpagiven,
       |    cast(ocpc_log_dict['dynamicbid'] as double) as bid,
       |    cast(ocpc_log_dict['kvalue'] as double) as kvalue,
       |    cast(ocpc_log_dict['conversiongoal'] as int) as conversion_goal,
       |    cast(ocpc_log_dict['ocpcstep'] as int) as ocpc_step,
       |    cast(ocpc_log_dict['IsHiddenOcpc'] as int) as is_hidden,
       |    cast(ocpc_log_dict['cvrCalFactor'] as double) as cali_value,
       |    cast(ocpc_log_dict['pcvr'] as double) as cali_pcvr,
       |    cast(ocpc_log_dict['postCvr'] as double) as cali_postcvr,
       |    cast(ocpc_log_dict['smoothFactor'] as double) as smooth_factor,
       |    cast(ocpc_log_dict['CpaSuggest'] as double) as cpa_suggest,
       |    hour as hr
       |FROM
       |    dl_cpc.ocpc_filter_unionlog
       |WHERE
       |    `date`='$date' and `hour` <= '$hour'
       |and is_ocpc=1
       |and media_appsid in ('80001098','80001292')
       |and round(adclass/1000) != 132101  --去掉互动导流
       |and isshow = 1
       |and ideaid > 0
       |and adsrc = 1
       |and adslot_type in (1,2,3)
       |and searchid is not null
       """.stripMargin
    println(sqlRequest)
    val rawData = spark
      .sql(sqlRequest)
      .withColumn("conversion_goal", when(col("conversion_goal") === 0, 1).otherwise(col("conversion_goal")))


    // 关联转化表
    val selectCondition = s"`date`='$date'"
    // cvr1
    val cvr1Data = spark
      .table("dl_cpc.ocpc_label_cvr_hourly")
      .where(selectCondition)
      .filter(s"cvr_goal = 'cvr1'")
      .select("searchid")
      .withColumn("iscvr1", lit(1))
      .distinct()

    // cvr2
    val cvr2Data = spark
      .table("dl_cpc.ocpc_label_cvr_hourly")
      .where(selectCondition)
      .filter(s"cvr_goal = 'cvr2'")
      .select("searchid")
      .withColumn("iscvr2", lit(1))
      .distinct()

    // cvr3
    val cvr3Data = spark
      .table("dl_cpc.ocpc_label_cvr_hourly")
      .where(selectCondition)
      .filter(s"cvr_goal = 'cvr3'")
      .select("searchid")
      .withColumn("iscvr3", lit(1))
      .distinct()

    // cvr4
    val cvr4Data = spark
      .table("dl_cpc.ocpc_label_cvr_hourly")
      .where(selectCondition)
      .filter(s"cvr_goal = 'cvr4'")
      .select("searchid")
      .withColumn("iscvr4", lit(1))
      .distinct()

    // 数据关联
    val resultDF = rawData
      .join(cvr1Data, Seq("searchid"), "left_outer")
      .join(cvr2Data, Seq("searchid"), "left_outer")
      .join(cvr3Data, Seq("searchid"), "left_outer")
      .join(cvr4Data, Seq("searchid"), "left_outer")
      .withColumn("iscvr", when(col("conversion_goal") === 1, col("iscvr1")).otherwise(when(col("conversion_goal") === 2, col("iscvr2"))
        .otherwise(when(col("conversion_goal") === 3, col("iscvr3")).otherwise(col("iscvr4")))))
      .select("searchid", "unitid", "userid", "isclick", "isshow", "price", "exp_ctr", "exp_cvr", "cpagiven", "bid", "kvalue", "conversion_goal", "ocpc_step", "hr", "iscvr1", "iscvr2", "iscvr3", "iscvr", "is_hidden", "cali_value", "cali_pcvr", "cali_postcvr", "smooth_factor", "cpa_suggest")

    resultDF.show(10)

    resultDF

  }


}
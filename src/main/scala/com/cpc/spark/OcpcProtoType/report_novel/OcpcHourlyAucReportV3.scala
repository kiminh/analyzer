package com.cpc.spark.OcpcProtoType.report_novel

import com.cpc.spark.OcpcProtoType.report.OcpcHourlyAucReport._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcHourlyAucReportV3 {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    Logger.getRootLogger.setLevel(Level.WARN)
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, media=$media")

    // spark app name
    val spark = SparkSession.builder().appName(s"OcpcHourlyAucReport: $date, $hour").enableHiveSupport().getOrCreate()

    var isHidden = 0
    if (version == "novel_v3") {
      isHidden = 0
    } else {
      isHidden = 1
    }

    val rawData = getOcpcLog2(media, date, hour, spark).filter(s"is_hidden = $isHidden")

    // 详情表数据
    val versionUnit = version + "_unitid"
    val unitData1 = calculateByUnitid(rawData, date, hour, spark)
    val unitData2 = calculateAUCbyUnitid(rawData, date, hour, spark)
    val unitData = unitData1
      .join(unitData2, Seq("unitid", "userid", "conversion_goal"), "left_outer")
      .withColumn("identifier", col("unitid"))
      .selectExpr("cast(identifier as string) identifier", "userid", "conversion_goal", "pre_cvr", "cast(post_cvr as double) post_cvr", "q_factor", "cpagiven", "cast(cpareal as double) cpareal", "cast(acp as double) acp", "acb", "auc")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(versionUnit))

    unitData
      .repartition(2).write.mode("overwrite").insertInto("dl_cpc.ocpc_auc_report_detail_hourly")


    // 详情表数据
    val versionUserid = version + "_userid"
    val userData1 = calculateByUserid(rawData, date, hour, spark)
    val userData2 = calculateAUCbyUserid(rawData, date, hour, spark)
    val userData = userData1
      .join(userData2, Seq("userid", "conversion_goal"), "left_outer")
      .withColumn("identifier", col("userid"))
      .selectExpr("cast(identifier as string) identifier", "userid", "conversion_goal", "pre_cvr", "cast(post_cvr as double) post_cvr", "q_factor", "cpagiven", "cast(cpareal as double) cpareal", "cast(acp as double) acp", "acb", "auc")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(versionUserid))

//    userData.write.mode("overwrite").saveAsTable("test.ocpc_detail_report_hourly20190226")
    userData
      .repartition(2).write.mode("overwrite").insertInto("dl_cpc.ocpc_auc_report_detail_hourly")

    // 汇总表数据
    val conversionData1 = calculateByConversionGoal(rawData, date, hour, spark)
    val conversionData2 = calculateAUCbyConversionGoal(rawData, date, hour, spark)
    val conversionData = conversionData1
      .join(conversionData2, Seq("conversion_goal"), "left_outer")
      .select("conversion_goal", "pre_cvr", "post_cvr", "q_factor", "cpagiven", "cpareal", "acp", "acb", "auc")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

//    conversionData.write.mode("overwrite").saveAsTable("test.ocpc_summary_report_hourly20190226")
    conversionData
      .repartition(1).write.mode("overwrite").insertInto("dl_cpc.ocpc_auc_report_summary_hourly")


  }

  def getOcpcLog2(media: String, date: String, hour: String, spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)
    val selectCondition = s"`date` = '$date' and `hour` <= '$hour'"

    // ctrData
    // todo
    val sqlRequest =
    s"""
       |SELECT
       |    searchid,
       |    unitid,
       |    userid,
       |    isclick,
       |    isshow,
       |    exp_cvr,
       |    cast(ocpc_log_dict['cpagiven'] as double) as cpagiven,
       |    cast(ocpc_log_dict['dynamicbid'] as double) as bid,
       |    cast(ocpc_log_dict['conversiongoal'] as int) as conversion_goal,
       |    cast(ocpc_log_dict['IsHiddenOcpc'] as int) as is_hidden,
       |    price
       |FROM
       |    dl_cpc.ocpc_filter_unionlog
       |WHERE
       |    $selectCondition
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
    val ctrData = spark
      .sql(sqlRequest)
      .withColumn("conversion_goal", when(col("conversion_goal") === 0, 1).otherwise(col("conversion_goal")))

    // cvr1Data
    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  1 as iscvr1
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  cvr_goal = 'cvr1'
       """.stripMargin
    println(sqlRequest1)
    val cvr1Data = spark.sql(sqlRequest1).distinct()

    // cvr2Data
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  1 as iscvr2
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  cvr_goal = 'cvr2'
       """.stripMargin
    println(sqlRequest2)
    val cvr2Data = spark.sql(sqlRequest2).distinct()

    // cvr3Data
    val sqlRequest3 =
      s"""
         |SELECT
         |  searchid,
         |  1 as iscvr3
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  cvr_goal = 'cvr3'
       """.stripMargin
    println(sqlRequest3)
    val cvr3Data = spark.sql(sqlRequest3).distinct()

    // cvr4Data
    val sqlRequest4 =
      s"""
         |SELECT
         |  searchid,
         |  1 as iscvr4
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  cvr_goal = 'cvr4'
       """.stripMargin
    println(sqlRequest4)
    val cvr4Data = spark.sql(sqlRequest4).distinct()

    // 数据关联
    val resultDF = ctrData
      .join(cvr1Data, Seq("searchid"), "left_outer")
      .join(cvr2Data, Seq("searchid"), "left_outer")
      .join(cvr3Data, Seq("searchid"), "left_outer")
      .join(cvr4Data, Seq("searchid"), "left_outer")
      .select("searchid", "unitid", "userid", "isclick", "isshow", "exp_cvr", "cpagiven", "bid", "conversion_goal", "price", "iscvr1", "iscvr2", "iscvr3", "is_hidden")
      .withColumn("iscvr", when(col("conversion_goal") === 1, col("iscvr1")).otherwise(when(col("conversion_goal") === 2, col("iscvr2"))
        .otherwise(when(col("conversion_goal") === 3, col("iscvr3")).otherwise(col("iscvr4")))))
      .select("searchid", "unitid", "userid", "isclick", "isshow", "exp_cvr", "cpagiven", "bid", "conversion_goal", "price", "iscvr", "is_hidden")
      .na.fill(0, Seq("iscvr"))

    resultDF


  }

}
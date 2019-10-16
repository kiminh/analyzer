package com.cpc.spark.oCPX.oCPC.report

import com.cpc.spark.OcpcProtoType.report.OcpcHourlyGeneralData._
import com.cpc.spark.oCPX.OcpcTools.{udfConcatStringInt, udfDetermineIndustry, udfDetermineMedia}
import com.cpc.spark.tools.OperateMySQL
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object OcpcHourlyGeneralData {
  def main(args: Array[String]): Unit = {
    /*
    新版报表程序
    CREATE TABLE `report_ocpc_general_data` (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `industry` varchar(255) NOT NULL DEFAULT '',
      `cost` double(11,6) NOT NULL DEFAULT '0.00000' COMMENT 'oCPC总消费',
      `cost_cmp` double(11,6) NOT NULL DEFAULT '0.00000' COMMENT 'oCPC消费环比',
      `cost_ratio` double(11,6) NOT NULL DEFAULT '0.00000' COMMENT 'oCPC消费占比',
      `cost_low` double(11,6) NOT NULL DEFAULT '0.00000' COMMENT 'oCPC正常消费',
      `cost_high` double(11,6) NOT NULL DEFAULT '0.00000' COMMENT 'oCPC超成本消费',
      `unitid_cnt` int(11) NOT NULL DEFAULT '0' COMMENT 'oCPC投放单元数',
      `userid_cnt` int(11) NOT NULL DEFAULT '0' COMMENT 'oCPC投放账户数',
      `date` date NOT NULL,
      `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (`id`) USING BTREE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

    1. 从base表抽取数据，按照行业过滤数据
    2. 统计各项指标
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


//    val clickCpcData = getCpcClickData(media, date, hour, spark)
//    val clickOcpcData = getOcpcClickData(media, date, hour, spark)
//    val cvData1 = getConversionData("cvr1", date, hour, spark)
//    val cvData2 = getConversionData("cvr2", date, hour, spark)
//    val cvData3 = getConversionData("cvr3", date, hour, spark)
//
//    val rawData = clickOcpcData
//      .join(cvData1, Seq("searchid"), "left_outer")
//      .join(cvData2, Seq("searchid"), "left_outer")
//      .join(cvData3, Seq("searchid"), "left_outer")
//      .select("searchid", "unitid", "userid", "isshow", "isclick", "price", "conversion_goal", "cpagiven", "is_api_callback", "industry", "iscvr1", "iscvr2", "iscvr3")
//      .withColumn("iscvr", when(col("conversion_goal") === 1, col("iscvr1")).otherwise(when(col("conversion_goal") === 2, col("iscvr2")).otherwise(col("iscvr3"))))

    val ocpcData = getOcpcData(date, hour, spark)
    val cpcData = getCpcData(date, hour, spark)

    // 统计汇总数据
    val cpcData = getCPCstats(cpcData, date, hour, spark)
    val ocpcData = getOCPCstats(ocpcData, date, hour, spark)

    val joinData = ocpcData
      .join(cpcData, Seq("industry"), "inner")

    // 计算前一天数据
    val result1 = joinData
      .withColumn("cost_cmp", lit(0.1))
      .withColumn("cost_ratio", col("ocpc_cost") * 1.0 / col("cost"))
      .withColumn("cost_low", col("low_cost") * 0.01)
      .withColumn("cost_high", col("high_cost") * 0.01)
      .withColumn("low_unit_percent", col("low_unitid_cnt") * 1.0 / col("unitid_cnt"))
      .withColumn("pay_percent", col("high_cost") * 1.0 / col("ocpc_cost"))

    val prevData = getPrevData(date, hour, spark)
    val result2 = result1
        .join(prevData, Seq("industry"), "left_outer")
        .na.fill(0.0, Seq("cost_yesterday"))
        .withColumn("cost_cmp", when(col("cost_yesterday") === 0.0, 1.0).otherwise((col("ocpc_cost") * 0.01 - col("cost_yesterday")) / col("cost_yesterday")))
    val result = result2

    result.show(10)

    val resultDF = result
      .withColumn("cost", col("ocpc_cost") * 0.01)
      .select("industry", "cost", "cost_cmp", "cost_ratio", "cost_low", "cost_high", "unitid_cnt", "userid_cnt", "low_unit_percent", "pay_percent", "cpa_real", "cpa_given")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
//      .cache()

    resultDF.show(10)

    resultDF
      .select("industry", "cost", "cost_cmp", "cost_ratio", "cost_low", "cost_high", "unitid_cnt", "userid_cnt", "low_unit_percent", "pay_percent", "date", "hour", "version")
//      .repartition(1).write.mode("overwrite").saveAsTable("test.ocpc_general_data_industry20190423")
      .repartition(1).write.mode("overwrite").insertInto("dl_cpc.ocpc_general_data_industry")

//    saveDataToMysql(resultDF, date, hour, spark)

  }

  def getOCPCstats(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    rawData.createOrReplaceTempView("raw_data")
    val sqlRequest =
      s"""
         |SELECT
         |  industry,
         |  unitid,
         |  userid,
         |  sum(case when isclick=1 then price else 0 end) as ocpc_cost,
         |  sum(iscvr) as cv,
         |  sum(case when isclick=1 then cpagiven else 0 end) * 1.0 / sum(isclick) as cpagiven
         |FROM
         |  raw_data
         |GROUP BY industry, unitid, userid
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    val baseData = data
      .withColumn("pred_cost", col("cv") * col("cpagiven"))
      .withColumn("high_cost", col("ocpc_cost") -  col("pred_cost") * 1.2)
      .withColumn("high_cost", when(col("high_cost") <= 0, 0.0).otherwise(col("high_cost")))

    //    baseData.write.mode("overwrite").saveAsTable("test.ocpc_general_data_industry20190423a")

    baseData.createOrReplaceTempView("base_data")
    val sqlRequest2 =
      s"""
         |SELECT
         |  industry,
         |  sum(ocpc_cost) as ocpc_cost,
         |  sum(high_cost) as high_cost,
         |  sum(case when high_cost = 0.0 then 1 else 0 end) as low_unitid_cnt,
         |  count(unitid) as unitid_cnt,
         |  count(distinct userid) as userid_cnt
         |FROM
         |  base_data
         |GROUP BY industry
       """.stripMargin
    println(sqlRequest2)
    val result1 = spark
      .sql(sqlRequest2)
      .withColumn("low_cost", col("ocpc_cost") - col("high_cost"))

    val sqlRequest3 =
      s"""
         |SELECT
         |  industry,
         |  sum(case when isclick=1 then cpagiven else 0 end) * 1.0 / sum(isclick) as cpa_given,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr) as cpa_real
         |FROM
         |  raw_data
         |GROUP BY industry
       """.stripMargin
    println(sqlRequest3)
    val result2 = spark.sql(sqlRequest3)

    val result = result1
      .join(result2, Seq("industry"), "inner")

    result

  }

  def getCPCstats(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    rawData.createOrReplaceTempView("raw_data")
    val sqlRequest =
      s"""
         |SELECT
         |  industry,
         |  conversion_goal,
         |  media,
         |  sum(case when isclick=1 then price else 0 end) as cost
         |FROM
         |  raw_data
         |GROUP BY industry, conversion_goal, media
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

  def getCpcData(date: String, hour: String, spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 抽取基础数据：所有跑ocpc的广告主
    val sqlRequest1 =
      s"""
         |SELECT
         |    searchid,
         |    ideaid,
         |    unitid,
         |    userid,
         |    adslot_type,
         |    adclass,
         |    conversion_goal,
         |    isclick,
         |    isshow,
         |    price,
         |    media_appsid
         |FROM
         |    dl_cpc.ocpc_base_unionlog
         |WHERE
         |    `date` = '$date'
         |and `hour` <= '$hour'
         |and $mediaSelection
         |and isshow = 1
         |and conversion_goal > 0
       """.stripMargin
    println(sqlRequest1)
    val result = spark
      .sql(sqlRequest1)
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))
      .withColumn("industry", udfDetermineIndustry()(col("adslot_type"), col("adclass")))

    result
  }

  def getOcpcData(date: String, hour: String, spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 抽取基础数据：所有跑ocpc的广告主
    val sqlRequest1 =
      s"""
         |SELECT
         |    searchid,
         |    ideaid,
         |    unitid,
         |    userid,
         |    adslot_type,
         |    adclass,
         |    conversion_goal,
         |    isclick,
         |    isshow,
         |    price,
         |    bid_discounted_by_ad_slot as bid,
         |    exp_cvr,
         |    exp_ctr,
         |    media_appsid,
         |    cast(ocpc_log_dict['cpagiven'] as double) as cpagiven
         |FROM
         |    dl_cpc.ocpc_filter_unionlog
         |WHERE
         |    `date` = '$date'
         |and `hour` <= '$hour'
         |and $mediaSelection
         |and isshow = 1
         |and conversion_goal > 0
       """.stripMargin
    println(sqlRequest1)
    val clickData = spark
      .sql(sqlRequest1)
      .withColumn("cvr_goal", udfConcatStringInt("cvr")(col("conversion_goal")))
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))
      .withColumn("industry", udfDetermineIndustry()(col("adslot_type"), col("adclass")))


    // 关联转化表
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr,
         |  cvr_goal
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  date = '$date'
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2).distinct()

    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "cvr_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))

    resultDF

  }

  def saveDataToMysql(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val hourInt = hour.toInt
    // 详情表
    val dataMysql = data
      .withColumn("cost", col("cost") * 100)
      .withColumn("cost_low", col("cost_low") * 100)
      .withColumn("cost_high", col("cost_high") * 100)
      .withColumn("cpa_ratio", col("cpa_real") * 1.0 / col("cpa_given"))
      .selectExpr("industry", "cast(cost as int) as cost", "cast(round(cost_cmp, 2) as double) as cost_cmp", "cast(round(cost_ratio, 2) as double) as cost_ratio", "cast(cost_low as int) as cost_low", "cast(cost_high as int) as cost_high", "cast(unitid_cnt as int) unitid_cnt", "cast(userid_cnt as int) userid_cnt", "cast(low_unit_percent as double) control_unit_ratio", "cast(pay_percent as double) uncontrol_pay_ratio", "cast(cpa_given as double) cpa_given", "cast(cpa_real as double) cpa_real", "cast(cpa_ratio as double) cpa_ratio")
      .na.fill(0, Seq("cost", "cost_cmp", "cost_ratio", "cost_low", "cost_high", "unitid_cnt", "userid_cnt", "control_unit_ratio", "uncontrol_pay_ratio", "cpa_given", "cpa_real", "cpa_ratio"))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hourInt))

    dataMysql.printSchema()
    val reportTableUnit = "report2.report_ocpc_general_data"
    val delSQLunit = s"delete from $reportTableUnit where `date` = '$date' and hour = $hourInt"

    OperateMySQL.update(delSQLunit) //先删除历史数据
    OperateMySQL.insert(dataMysql, reportTableUnit) //插入数据
  }



}

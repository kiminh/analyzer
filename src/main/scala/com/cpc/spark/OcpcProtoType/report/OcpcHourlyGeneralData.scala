package com.cpc.spark.OcpcProtoType.report

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.cpc.spark.tools.OperateMySQL
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


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


    val clickCpcData = getCpcClickData(media, date, hour, spark)
    val clickOcpcData = getOcpcClickData(media, date, hour, spark)
    val cvData1 = getConversionData("cvr1", date, hour, spark)
    val cvData2 = getConversionData("cvr2", date, hour, spark)
    val cvData3 = getConversionData("cvr3", date, hour, spark)

    val rawData = clickOcpcData
      .join(cvData1, Seq("searchid"), "left_outer")
      .join(cvData2, Seq("searchid"), "left_outer")
      .join(cvData3, Seq("searchid"), "left_outer")
      .select("searchid", "unitid", "userid", "isshow", "isclick", "price", "conversion_goal", "cpagiven", "is_api_callback", "industry", "iscvr1", "iscvr2", "iscvr3")
      .withColumn("iscvr", when(col("conversion_goal") === 1, col("iscvr1")).otherwise(when(col("conversion_goal") === 2, col("iscvr2")).otherwise(col("iscvr3"))))

    rawData.show(10)

    // 统计汇总数据
    val cpcData = getCPCstats(clickCpcData, date, hour, spark)
    val ocpcData = getOCPCstats(rawData, date, hour, spark)

    val joinData = ocpcData
      .join(cpcData, Seq("industry"), "inner")

    // 计算前一天数据
    val result = joinData
      .withColumn("cost_cmp", lit(0.1))
      .withColumn("cost_ratio", col("ocpc_cost") * 1.0 / col("cost"))
      .withColumn("cost_low", col("low_cost"))
      .withColumn("cost_high", col("high_cost"))
      .withColumn("cost", col("ocpc_cost"))
      .select("industry", "cost", "cost_cmp", "cost_ratio", "cost_low", "cost_high", "unitid_cnt", "userid_cnt")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

    result
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_general_data_industry")


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
         |  sum(case when isclick=1 then cpagiven else 0 end) as cpagiven
         |FROM
         |  raw_data
         |GROUP BY industry, unitid, userid
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    val baseData = data
      .withColumn("pred_cost", col("cv") * col("cpagiven"))
      .withColumn("high_cost", col("cost") -  col("pred_cost") * 1.2)
      .withColumn("high_cost", when(col("high_cost") <= 0, 0.0).otherwise(col("high_cost")))

    baseData.createOrReplaceTempView("base_data")
    val sqlRequest2 =
      s"""
         |SELECT
         |  industry,
         |  sum(ocpc_cost) as ocpc_cost,
         |  sum(high_cost) as high_cost,
         |  count(distinct unitid) as unitid_cnt,
         |  count(distinct userid) as userid_cnt
         |FROM
         |  base_data
         |GROUP BY industry
       """.stripMargin
    println(sqlRequest2)
    val result = spark
      .sql(sqlRequest2)
      .withColumn("low_cost", col("ocpc_cost") - col("high_cost"))

    result

  }

  def getCPCstats(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    rawData.createOrReplaceTempView("raw_data")
    val sqlRequest =
      s"""
         |SELECT
         |  industry,
         |  sum(case when isclick=1 then price else 0 end) as cost
         |FROM
         |  raw_data
         |GROUP BY industry
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

  def getConversionData(cvrType: String, date: String, hour: String, spark: SparkSession) = {
    val selectCondition = s"`date` >= '$date' and cvr_goal = '$cvrType'"

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  label
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest)
    val labelCol = "is" + cvrType
    val data = spark
      .sql(sqlRequest)
      .withColumn(labelCol, col("label"))

    data
  }

  def getOcpcClickData(media: String, date: String, hour: String, spark: SparkSession) = {
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 取历史数据
    val selectCondition = s"`date` = '$date' and `hour` <= '$hour'"

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  userid,
         |  isshow,
         |  isclick,
         |  price,
         |  cast(ocpc_log_dict['conversiongoal'] as int) as conversion_goal,
         |  cast(ocpc_log_dict['cpagiven'] as double) as cpagiven,
         |  is_api_callback,
         |  (case
         |    when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |    when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |    when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |    when adclass in (110110100, 125100100) then "wzcp"
         |    else "others"
         |  end) as industry
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  isclick = 1
         |AND
         |  is_ocpc = 1
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    val appData = data.filter(s"is_api_callback = 1 and industry = 'feedapp' and conversion_goal = 2")
    val eldsData = data.filter(s"industry = 'elds' and conversion_goal = 3")
    val resultDF = appData.union(eldsData)

    resultDF
  }

  def getCpcClickData(media: String, date: String, hour: String, spark: SparkSession) = {
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 取历史数据
    val selectCondition = s"`date` = '$date' and `hour` <= '$hour'"

    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  userid,
         |  isshow,
         |  isclick,
         |  price,
         |  is_api_callback,
         |  (case
         |    when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |    when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |    when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |    when adclass in (110110100, 125100100) then "wzcp"
         |    else "others"
         |  end) as industry
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  isclick = 1
       """.stripMargin
    println(sqlRequest1)
    val data = spark.sql(sqlRequest1)

    val appData = data.filter(s"is_api_callback = 1 and industry = 'feedapp'")
    val eldsData = data.filter(s"industry = 'elds'")
    val resultDF = appData.union(eldsData)

    resultDF
  }



}

package com.cpc.spark.OcpcProtoType.report_qtt

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.tools._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.OcpcProtoType.report.OcpcHourlyGeneralData._


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

//    rawData.show(10)

    // 统计汇总数据
    val cpcData = getCPCstats(clickCpcData, date, hour, spark)
    val ocpcData = getOCPCstats(rawData, date, hour, spark)

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
      .select("industry", "cost", "cost_cmp", "cost_ratio", "cost_low", "cost_high", "unitid_cnt", "userid_cnt", "low_unit_percent", "pay_percent")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
//      .cache()

    resultDF.show(10)

    resultDF
//      .repartition(1).write.mode("overwrite").saveAsTable("test.ocpc_general_data_industry20190423")
      .repartition(1).write.mode("overwrite").insertInto("dl_cpc.ocpc_general_data_industry")

    saveDataToMysql(resultDF, date, hour, spark)

  }
}

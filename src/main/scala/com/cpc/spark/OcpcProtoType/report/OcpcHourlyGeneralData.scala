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


    val clickData = getClickData(media, date, hour, spark)
    val cvData1 = getConversionData("cvr1", date, hour, spark)
    val cvData2 = getConversionData("cvr2", date, hour, spark)
    val cvData3 = getConversionData("cvr3", date, hour, spark)

//    searchid,
//    unitid,
//    userid,
//    isshow,
//    isclick,
//    price,
//    exp_cvr,
//    conversion_goal,
//    is_ocpc
    val data = clickData
      .join(cvData1, Seq("searchid"), "left_outer")
      .join(cvData2, Seq("searchid"), "left_outer")
      .join(cvData3, Seq("searchid"), "left_outer")
      .select("searchid", "unitid", "userid", "isshow", "isclick", "price", "exp_cvr", "conversion_goal", "is_ocpc", "iscvr1", "iscvr2", "iscvr3")
      .withColumn("iscvr", when(col("conversion_goal") === 1, col("iscvr1")))

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

  def getClickData(media: String, date: String, hour: String, spark: SparkSession) = {
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
         |  exp_cvr,
         |  conversion_goal,
         |  is_ocpc
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  isclick = 1
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)

    resultDF
  }



}

package com.cpc.spark.OcpcProtoType.temp_file

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.cpc.spark.ocpcV3.ocpc.OcpcUtils.getTimeRangeSql3
import com.cpc.spark.tools.OperateMySQL
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import com.typesafe.config.ConfigFactory
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable


object OcpcTempCalcAuc {
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

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  2 as conversion_goal,
         |  exp_cvr,
         |  conversion as iscvr
         |FROM
         |  spring.auc_tmp
       """.stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .na.fill(0, Seq("iscvr"))

    val result = calculateAUCbyConversionGoal(data, date, hour, spark)
    result.show(10)

  }

  def calculateAUCbyConversionGoal(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val key = data.select("conversion_goal").distinct()
    val aucList = new mutable.ListBuffer[(Int, Double)]()
    var cnt = 0

    for (row <- key.collect()) {
      val conversion_goal = row.getAs[Int]("conversion_goal")
      val selectCondition = s"conversion_goal=$conversion_goal"
      println(selectCondition)
      val singleData = data
        .withColumn("score", col("exp_cvr"))
        .withColumn("label", col("iscvr"))
        .filter(selectCondition)
      val scoreAndLabel = singleData
        .select("score", "label")
        .rdd
        .map(x=>(x.getAs[Double]("score").toDouble, x.getAs[Int]("label").toDouble))
      val scoreAndLabelNum = scoreAndLabel.count()
      if (scoreAndLabelNum > 0) {
        val metrics = new BinaryClassificationMetrics(scoreAndLabel)
        val aucROC = metrics.areaUnderROC
        println(s"### result is $aucROC, cnt=$cnt ###")
        aucList.append((conversion_goal, aucROC))
      }
      cnt += 1
    }

    val resultDF = spark
      .createDataFrame(aucList)
      .toDF("conversion_goal", "auc")

    resultDF

  }

}

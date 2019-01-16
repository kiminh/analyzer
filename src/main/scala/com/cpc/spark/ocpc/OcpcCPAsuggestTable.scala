package com.cpc.spark.ocpcV3.ocpc.model

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils._
import com.cpc.spark.ocpc.utils.OcpcUtils.getIdeaUpdates
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import com.cpc.spark.udfs.Udfs_wj._
import com.typesafe.config.ConfigFactory


object OcpcCPAsuggestTable {
  def main(args: Array[String]): Unit = {
    /*
    每天根据是否有ocpc的广告记录更新cpc阶段的推荐cpa并存储到pb文件中
    1. 根据日期抽取前一天的推荐cpa
    2. 从ocpc_union_log_hourly判断是否有ocpc广告记录，并将标签（ocpc_flag）关联到推荐cpa表上
    3. 将推荐cpa表与dl_cpc.ocpc_cpc_cpa_suggest_hourly进行外关联
    4. 根据ocpc_flag判断是否更新cpa_suggest和t
      a. 如果ocpc_flag=1即当天有ocpc广告记录，则更新cpa_suggest和t: t = 1/sqrt(day)
      b. 如果ocpc_flag=0或null，则不更新cpa_suggest和t
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString


  }

}


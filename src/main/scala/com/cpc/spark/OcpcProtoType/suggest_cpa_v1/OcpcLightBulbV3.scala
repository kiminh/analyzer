package com.cpc.spark.OcpcProtoType.suggest_cpa_v1

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils._
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods._




object OcpcLightBulbV3{
  def main(args: Array[String]): Unit = {
    /*
    通过向redis中存储suggest cpa来控制灯泡的亮灯逻辑
    1. 抽取recommendation数据表
    2. mappartition打开redis，并存储数据
     */
    // 计算日期周期
//    2019-02-02 10 qtt_demo qtt
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString


    val spark = SparkSession
      .builder()
      .appName(s"OcpcLightBulbV2: $date, $hour, $version, $media")
      .enableHiveSupport().getOrCreate()

    // todo 修改表名
    val tableName1 = "dl_cpc.ocpc_light_control_version"
    val tableName2 = "dl_cpc.ocpc_light_control_daily"

    val result = getUpdateTable(tableName1, tableName2, date, version, spark)


  }

  def getUpdateTable(table1: String, table2: String, date: String, version: String, spark: SparkSession) = {

  }



}

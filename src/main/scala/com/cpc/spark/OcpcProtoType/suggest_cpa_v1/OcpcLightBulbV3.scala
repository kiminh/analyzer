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
    val tableName = "dl_cpc.ocpc_light_control_version"


    val jsonObj = new JSONObject
    jsonObj.put("unit_id",270)
    jsonObj.put("ocpc_light",1)
    jsonObj.put("ocpc_suggest_price", "1.5")
    val url = "service.aiclk.com/ocpc/update"
    val client = new HttpClient();
    client.getHostConfiguration().setHost(url)
//    val post =
//    val client = HttpClient.createDefault()
//    val post: HttpPost = new HttpPost(url)

  }



}

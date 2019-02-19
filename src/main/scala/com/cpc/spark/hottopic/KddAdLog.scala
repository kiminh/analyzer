package com.cpc.spark.hottopic

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2019/2/15 15:08
  */
object KddAdLog {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val hour = args(1)

        val spark = SparkSession.builder()
          .appName(s"KddAdLog date = $date , hour = $hour")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val sql =
            s"""
               |select field["device"].string_type as uid,
               |--请求广告类型
               |  if (field["cmd"].string_type = "161" and field["from"].string_type = "1", field["type"].string_type, null) as cpc_req_type,
               |  if (field["cmd"].string_type = "161" and field["from"].string_type = "2", field["type"].string_type, null) as csj_req_type,
               |  if (field["cmd"].string_type = "161" and field["from"].string_type = "3", field["type"].string_type, null) as gdt_req_type,
               |--请求广告位置
               |  if (field["cmd"].string_type = "161" and field["from"].string_type = "1", field["position"].string_type, null) as cpc_req_position,
               |  if (field["cmd"].string_type = "161" and field["from"].string_type = "2", field["position"].string_type, null) as csj_req_position,
               |  if (field["cmd"].string_type = "161" and field["from"].string_type = "3", field["position"].string_type, null) as gdt_req_position,
               |--请求广告标题
               |  if (field["cmd"].string_type = "161" and field["from"].string_type = "1", field["title"].string_type, null) as cpc_req_title,
               |  if (field["cmd"].string_type = "161" and field["from"].string_type = "2", field["title"].string_type, null) as csj_req_title,
               |  if (field["cmd"].string_type = "161" and field["from"].string_type = "3", field["title"].string_type, null) as gdt_req_title,
               |--展示广告类型
               |  if (field["cmd"].string_type = "162" and field["from"].string_type = "1", field["type"].string_type, null) as cpc_show_type,
               |  if (field["cmd"].string_type = "162" and field["from"].string_type = "2", field["type"].string_type, null) as csj_show_type,
               |  if (field["cmd"].string_type = "162" and field["from"].string_type = "3", field["type"].string_type, null) as gdt_show_type,
               |--展示广告位置
               |  if (field["cmd"].string_type = "162" and field["from"].string_type = "1", field["position"].string_type, null) as cpc_show_position,
               |  if (field["cmd"].string_type = "162" and field["from"].string_type = "2", field["position"].string_type, null) as csj_show_position,
               |  if (field["cmd"].string_type = "162" and field["from"].string_type = "3", field["position"].string_type, null) as gdt_show_position,
               |--展示广告标题
               |  if (field["cmd"].string_type = "162" and field["from"].string_type = "1", field["title"].string_type, null) as cpc_show_title,
               |  if (field["cmd"].string_type = "162" and field["from"].string_type = "2", field["title"].string_type, null) as csj_show_title,
               |  if (field["cmd"].string_type = "162" and field["from"].string_type = "3", field["title"].string_type, null) as gdt_show_title,
               |--点击广告类型
               |  if (field["cmd"].string_type = "163" and field["from"].string_type = "1", field["type"].string_type, null) as cpc_click_type,
               |  if (field["cmd"].string_type = "163" and field["from"].string_type = "2", field["type"].string_type, null) as csj_click_type,
               |  if (field["cmd"].string_type = "163" and field["from"].string_type = "3", field["type"].string_type, null) as gdt_click_type,
               |--点击广告位置
               |  if (field["cmd"].string_type = "163" and field["from"].string_type = "1", field["position"].string_type, null) as cpc_click_position,
               |  if (field["cmd"].string_type = "163" and field["from"].string_type = "2", field["position"].string_type, null) as csj_click_position,
               |  if (field["cmd"].string_type = "163" and field["from"].string_type = "3", field["position"].string_type, null) as gdt_click_position,
               |--点击广告标题
               |  if (field["cmd"].string_type = "163" and field["from"].string_type = "1", field["title"].string_type, null) as cpc_click_title,
               |  if (field["cmd"].string_type = "163" and field["from"].string_type = "2", field["title"].string_type, null) as csj_click_title,
               |  if (field["cmd"].string_type = "163" and field["from"].string_type = "3", field["title"].string_type, null) as gdt_click_title,
               |  '$date' as `date`,
               |  '$hour' as hour
               |from src_kanduoduo.kanduoduo_client_log
               |where thedate='$date' and thehour='$hour'
               |and field["cmd"].string_type in  ("160", "161", "162", "163", "165")
               |and field["device"].string_type not like "%.%"
               |and field["device"].string_type not like "%000000%"
               |and length(field["device"].string_type) in (14, 15, 36)
             """.stripMargin

        val kdd = spark.sql(sql)

        println("kdd 's num is " + kdd.count())

        kdd.repartition(10)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.kdd_ad_log")

        val sqltitle =
            s"""
               |select cpc_req_title as title from dl_cpc.kdd_ad_log where `date`='$date' and hour = '$hour' and length(cpc_req_title) > 0
               |union
               |select csj_req_title as title from dl_cpc.kdd_ad_log where `date`='$date' and hour = '$hour' and length(csj_req_title) > 0
               |union
               |select gdt_req_title as title from dl_cpc.kdd_ad_log where `date`='$date' and hour = '$hour' and length(gdt_req_title) > 0
               |union
               |select cpc_show_title as title from dl_cpc.kdd_ad_log where `date`='$date' and hour = '$hour' and length(cpc_show_title) > 0
               |union
               |select csj_show_title as title from dl_cpc.kdd_ad_log where `date`='$date' and hour = '$hour' and length(csj_show_title) > 0
               |union
               |select gdt_show_title as title from dl_cpc.kdd_ad_log where `date`='$date' and hour = '$hour' and length(gdt_show_title) > 0
               |union
               |select cpc_click_title as title from dl_cpc.kdd_ad_log where `date`='$date' and hour = '$hour' and length(cpc_click_title) > 0
               |union
               |select csj_click_title as title from dl_cpc.kdd_ad_log where `date`='$date' and hour = '$hour' and length(csj_click_title) > 0
               |union
               |select gdt_click_title as title from dl_cpc.kdd_ad_log where `date`='$date' and hour = '$hour' and length(gdt_click_title) > 0
             """.stripMargin

        val title = spark.sql(sqltitle).rdd.map(x => x.getAs[String]("title")).distinct()

        val dt = date.replace("-","") + "_" + hour

        val savePath = s"hdfs://emr-cluster/warehouse/kdd_ad_title/$dt"

        title.repartition(1).saveAsTextFile(savePath)
    }

}

/**
  create table if not exists dl_cpc.kdd_ad_log
  (
      uid string,
      cpc_req_type string comment 'cpc请求广告类型',
      csj_req_type string comment '穿山甲请求广告类型',
      gdt_req_type string comment '广点通请求广告类型',
      cpc_req_position string comment 'cpc请求广告位置',
      csj_req_position string comment '穿山甲请求广告位置',
      gdt_req_position string comment '广点通请求广告位置',
      cpc_req_title string comment 'cpc请求广告标题',
      csj_req_title string comment '穿山甲请求广告标题',
      gdt_req_title string comment '广点通请求广告标题',

      cpc_show_type string comment 'cpc展示广告类型',
      csj_show_type string comment '穿山甲展示广告类型',
      gdt_show_type string comment '广点通展示广告类型',
      cpc_show_position string comment 'cpc展示广告位置',
      csj_show_position string comment '穿山甲展示广告位置',
      gdt_show_position string comment '广点通展示广告位置',
      cpc_show_title string comment 'cpc展示广告标题',
      csj_show_title string comment '穿山甲展示广告标题',
      gdt_show_title string comment '广点通展示广告标题',

      cpc_click_type string comment 'cpc点击广告类型',
      csj_click_type string comment '穿山甲点击广告类型',
      gdt_click_type string comment '广点通点击广告类型',
      cpc_click_position string comment 'cpc点击广告位置',
      csj_click_position string comment '穿山甲点击广告位置',
      gdt_click_position string comment '广点通点击广告位置',
      cpc_click_title string comment 'cpc点击广告标题',
      csj_click_title string comment '穿山甲点击广告标题',
      gdt_click_title string comment '广点通点击广告标题'
  )
  PARTITIONED by (`date` string, hour string)
  STORED as PARQUET;
  */
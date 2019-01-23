package com.cpc.spark.hottopic

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2019/1/21 19:46
  */
object KddData {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"KddData date = $date")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val sql =
            s"""
               |select field["device"].string_type as uid,
               |  if (field["cmd"].string_type = "160", field["from"].string_type, null) as req_from,
               |  if (field["cmd"].string_type = "160", field["type"].string_type, null) as req_type,
               |  if (field["cmd"].string_type = "160", field["position"].string_type, null) as req_position,
               |  if (field["cmd"].string_type = "160", field["slot_id"].string_type, null) as req_slot_id,
               |  if (field["cmd"].string_type = "161", field["from"].string_type, null) as valid_req_from,
               |  if (field["cmd"].string_type = "161", field["type"].string_type, null) as valid_req_type,
               |  if (field["cmd"].string_type = "161", field["position"].string_type, null) as valid_req_position,
               |  if (field["cmd"].string_type = "161", field["slot_id"].string_type, null) as valid_req_slot_id,
               |  if (field["cmd"].string_type = "161", field["title"].string_type, null) as valid_req_title,
               |  if (field["cmd"].string_type = "161", field["source"].string_type, null) as valid_req_source,
               |  if (field["cmd"].string_type = "162", field["from"].string_type, null) as show_from,
               |  if (field["cmd"].string_type = "162", field["type"].string_type, null) as show_type,
               |  if (field["cmd"].string_type = "162", field["position"].string_type, null) as show_position,
               |  if (field["cmd"].string_type = "162", field["item"].string_type, null) as show_item,
               |  if (field["cmd"].string_type = "162", field["slot_id"].string_type, null) as show_slot_id,
               |  if (field["cmd"].string_type = "162", field["title"].string_type, null) as show_title,
               |  if (field["cmd"].string_type = "162", field["source"].string_type, null) as show_source,
               |  if (field["cmd"].string_type = "162", field["fromEx"].string_type, null) as show_fromex,
               |  if (field["cmd"].string_type = "163", field["from"].string_type, null) as click_from,
               |  if (field["cmd"].string_type = "163", field["type"].string_type, null) as click_type,
               |  if (field["cmd"].string_type = "163", field["position"].string_type, null) as click_ad_position,
               |  if (field["cmd"].string_type = "163", field["click_postion"].string_type, null) as click_postion,
               |  if (field["cmd"].string_type = "163", field["brush"].string_type, null) as click_brush,
               |  if (field["cmd"].string_type = "163", field["item"].string_type, null) as click_item,
               |  if (field["cmd"].string_type = "163", field["slot_id"].string_type, null) as click_slot_id,
               |  if (field["cmd"].string_type = "163", field["title"].string_type, null) as click_title,
               |  if (field["cmd"].string_type = "163", field["source"].string_type, null) as click_source,
               |  if (field["cmd"].string_type = "165", field["from"].string_type, null) as pg_from,
               |  if (field["cmd"].string_type = "165", field["type"].string_type, null) as pg_type,
               |  if (field["cmd"].string_type = "165", field["position"].string_type, null) as pg_position,
               |  if (field["cmd"].string_type = "165", field["slot_id"].string_type, null) as pg_slot_id,
               |  if (field["cmd"].string_type = "165", field["code"].string_type, null) as pg_code,
               |  if (field["cmd"].string_type = "165", field["error_msg"].string_type, null) as pg_error_msg,
               |  if (field["cmd"].string_type = "165", field["count"].string_type, null) as pg_count,
               |  thedate as `date`
               |from src_kanduoduo.kanduoduo_client_log
               |where thedate='$date'
               |and field["cmd"].string_type in  ("160", "161", "162", "163", "165")
               |and field["device"].string_type not like "%.%"
               |and field["device"].string_type not like "%000000%"
               |and length(field["device"].string_type) in (14, 15, 36)
             """.stripMargin

        val kdd = spark.sql(sql)

        println("kdd 's num is " + kdd.count())

        kdd.repartition(200)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.kdd_uid_data")
    }
}

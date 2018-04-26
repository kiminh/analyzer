package com.cpc.spark.stats.anal

import org.apache.spark.rdd.RDD

object MediaUVAnal {
  def anal(as: RDD[((Int, Int, Int, Int, Int, Int, Int, Int, Int, String, String, String), (Int, Int, Int, Int, Int, Int, Int, Int))]): RDD[String] = {
    val uv_hourly_as = as.map {
      case ((media_id, adslot_id, idea_id, unit_id, plan_id, os_id, country, province, city, uid, date, hour), (req, fill, imp, click, charge_click, spam_click, balance, coupon)) =>
        ((media_id, adslot_id, uid, date, hour), 1)
    }.distinct().reduceByKey {
      case (x, y) =>
        x + y
    }.filter {
      case ((media_id, adslot_id, uid, date, hour), num) =>
        media_id != 0
    }.map {
      case ((media_id, adslot_id, uid, date, hour), num) =>
        media_id + "|" + adslot_id + "|" + uid + "|" + date + "|" + hour + "|" + num
    }
    uv_hourly_as
  }
}
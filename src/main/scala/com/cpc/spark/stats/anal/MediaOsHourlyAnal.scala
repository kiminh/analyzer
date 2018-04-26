package com.cpc.spark.stats.anal

import org.apache.spark.rdd.RDD

object MediaOsHourlyAnal {
  def anal(as: RDD[((Int, Int, Int, Int, Int, Int, Int, Int, Int, String, String, String), (Int, Int, Int, Int, Int, Int, Int, Int))], show: RDD[((Int, Int, Int, Int, Int, Int, Int, Int, Int, String, String), (Int, Int, Int, Int, Int, Int, Int, Int))], click: RDD[((Int, Int, Int, Int, Int, Int, Int, Int, Int, String, String), (Int, Int, Int, Int, Int, Int, Int, Int))]): RDD[String] = {
    val os_hourly_as = as.map {
      case ((media_id, adslot_id, idea_id, unit_id, plan_id, os_id, country, province, city, uid, date, hour), (req, fill, imp, click, charge_click, spam_click, balance, coupon)) =>
        ((media_id, adslot_id, idea_id, unit_id, plan_id, os_id, date, hour), (req, fill, imp, click, charge_click, spam_click, balance, coupon))
    }
    val os_hourly_show = show.map {
      case ((media_id, adslot_id, idea_id, unit_id, plan_id, os_id, country, province, city, date, hour), (req, fill, imp, click, charge_click, spam_click, balance, coupon)) =>
        ((media_id, adslot_id, idea_id, unit_id, plan_id, os_id, date, hour), (req, fill, imp, click, charge_click, spam_click, balance, coupon))
    }
    val os_hourly_click = click.map {
      case ((media_id, adslot_id, idea_id, unit_id, plan_id, os_id, country, province, city, date, hour), (req, fill, imp, click, charge_click, spam_click, balance, coupon)) =>
        ((media_id, adslot_id, idea_id, unit_id, plan_id, os_id, date, hour), (req, fill, imp, click, charge_click, spam_click, balance, coupon))
    }
    val os_hourly = os_hourly_as.union(os_hourly_show).union(os_hourly_click)
    val os_store_hourly = os_hourly.reduceByKey {
      case (x, y) =>
        (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6, x._7 + y._7, x._8 + y._8)
    }.filter {
      case ((media_id, adslot_id, idea_id, unit_id, plan_id, os_id, date, hour), (req, fill, imp, click, charge_click, spam_click, balance, coupon)) =>
        media_id != 0
    }.map {
      case ((media_id, adslot_id, idea_id, unit_id, plan_id, os_id, date, hour), (req, fill, imp, click, charge_click, spam_click, balance, coupon)) =>
        media_id + "|" + adslot_id + "|" + idea_id + "|" + unit_id + "|" + plan_id + "|" + os_id + "|" + date + "|" + hour + "|" + req + "|" + fill + "|" + imp + "|" + click + "|" + charge_click + "|" + spam_click + "|" + balance + "|" + coupon
    }
    os_store_hourly
  }
}
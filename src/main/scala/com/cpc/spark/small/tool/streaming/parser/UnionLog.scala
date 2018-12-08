package com.cpc.spark.small.tool.streaming.parser

/**
  * Created by Roy on 2017/4/25.
  */
case class UnionLog(
                     var searchid: String = "",
                     var timestamp: Int = 0,
                     var network: Int = 0,
                     var ip: String = "",
                     var exptags: String = "",
                     var media_type: Int = 0,
                     var media_appsid: String = "0",
                     var adslotid: String = "0",
                     var adslot_type: Int = 0,
                     var adnum: Int = 0,
                     var isfill: Int = 0,
                     var adtype: Int = 0,
                     var adsrc: Int = 0,
                     var interaction: Int = 0,
                     var bid: Int = 0,
                     var floorbid: Float = 0,
                     var cpmbid: Float = 0,
                     var price: Int = 0,
                     var ctr: Long = 0,
                     var cpm: Long = 0,
                     var ideaid: Int = 0,
                     var unitid: Int = 0,
                     var planid: Int = 0,
                     var country: Int = 0,
                     var province: Int = 0,
                     var city: Int = 0,
                     var isp: Int = 0,
                     var brand: String = "",
                     var model: String = "",
                     var uid: String = "",
                     var ua: String = "",
                     var os: Int = 0,
                     var screen_w: Int = 0,
                     var screen_h: Int = 0,
                     var sex: Int = 0,
                     var age: Int = 0,
                     var coin: Int = 0,
                     var isshow: Int = 0,
                     var show_timestamp: Int = 0,
                     var show_network: Int = 0,
                     var show_ip: String = "",
                     var isclick: Int = 0,
                     var click_timestamp: Int = 0,
                     var click_network: Int = 0,
                     var click_ip: String = "",
                     var antispam_score: Int = 0,
                     var antispam_rules: String = "",
                     var duration: Int = 0,
                     var userid: Int = 0,
                     var interests: String = "",
                     var ext: collection.Map[String, ExtValue] = null,
                     var ext_int: collection.Map[String, Long] = null,
                     var ext_string: collection.Map[String, String] = null,
                     var ext_float: collection.Map[String, Double] = null,
                     var motivation: Seq[Motivation] = null,
                     var motive_ext: Seq[Map[String, String]] = null,
                     var date: String = "",
                     var hour: String = ""
                   ) {

  def spamClick(): Int = {
    if (ext != null) {
      ext.getOrElse("spam_click", ExtValue()).int_value
    } else {
      0
    }
  }

  def isSpamClick(): Int = {
    if (antispam_score < 10000 && isclick > 0) {
      1
    } else {
      0
    }
  }

  def isCharged(): Int = {
    if (price > 0 && isclick > 0 && antispam_score == 10000) {
      1
    } else {
      0
    }
  }

  def realCost(): Int = {
    if (isclick > 0 && antispam_score == 10000) {
      price
    } else {
      0
    }
  }
}

case class ExtValue(int_value: Int = 0, long_value: Long = 0, float_value: Float = 0, string_value: String = "")


case class Motivation(userid: Int = 0, planid: Int = 0, unitid: Int = 0, ideaid: Int = 0, bid: Int = 0, price: Int = 0,
                      isfill: Int = 0, isshow: Int = 0, isclick: Int = 0)
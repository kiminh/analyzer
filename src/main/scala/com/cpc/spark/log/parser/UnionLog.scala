package com.cpc.spark.log.parser


/**
  * Created by Roy on 2017/4/25.
  */
case class UnionLog(
                     searchid: String = "",
                     timestamp: Int = 0,
                     network: Int = 0,
                     ip: String = "",
                     exptags: String = "",
                     media_type: Int = 0,
                     media_appsid: String = "0",
                     adslotid: String = "0",
                     adslot_type: Int = 0,
                     adnum: Int = 0,
                     isfill: Int = 0,
                     adtype: Int = 0,
                     adsrc: Int = 0,
                     interaction: Int = 0,
                     bid: Int = 0,
                     floorbid: Float = 0,
                     cpmbid: Float = 0,
                     price: Int = 0,
                     ctr: Long = 0,
                     cpm: Long = 0,
                     ideaid: Int = 0,
                     unitid: Int = 0,
                     planid: Int = 0,
                     country: Int = 0,
                     province: Int = 0,
                     city: Int = 0,
                     isp: Int = 0,
                     brand: String = "",
                     model: String = "",
                     uid: String = "",
                     ua: String = "",
                     os: Int = 0,
                     screen_w: Int = 0,
                     screen_h: Int = 0,
                     sex: Int = 0,
                     age: Int = 0,
                     coin: Int = 0,
                     isshow: Int = 0,
                     show_timestamp: Int = 0,
                     show_network: Int = 0,
                     show_ip: String = "",
                     isclick: Int = 0,
                     click_timestamp: Int = 0,
                     click_network: Int = 0,
                     click_ip: String = "",
                     antispam_score: Int = 0,
                     antispam_rules: String = "",
                     duration: Int = 0,
                     userid: Int = 0,
                     interests: String = "",
                     ext: collection.Map[String, ExtValue] = null,
                     date: String = "",
                     hour: String = ""
                   ) {


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

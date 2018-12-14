package com.cpc.spark.small.tool.streaming.parser

/**
  * click日志
  *
  * @param searchid
  * @param isclick
  * @param ideaid
  * @param click_timestamp
  * @param antispam_score
  * @param antispam_rules
  * @param click_ip
  * @param touch_x
  * @param touch_y
  * @param slot_width
  * @param slot_height
  * @param antispam_predict
  * @param click_ua
  */
case class ClickLog(
                           var searchid: String = "",
                           var isclick: Int = 0,
                           var ideaid: Int = 0,
                           var click_timestamp: Int = 0,
                           var antispam_score: Int = 0,
                           var antispam_rules: String = "",
                           var click_ip: String = "",
                           var click_network: Int = 0,
                           var touch_x: Int = 0,
                           var touch_y: Int = 0,
                           var slot_width: Int = 0,
                           var slot_height: Int = 0,
                           var antispam_predict: Float = 0,
                           var click_ua: String = "",
                           var spam_click: Int = 0

//                           var ext: collection.Map[String, ExtValue] = null
                         ) {

  def isSpamClick(): Int = {
    if (antispam_score < 10000 && isclick > 0) {
      1
    } else {
      0
    }
  }

}


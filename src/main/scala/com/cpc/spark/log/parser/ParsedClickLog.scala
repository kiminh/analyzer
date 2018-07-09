package com.cpc.spark.log.parser

/**
  * click日志
  *
  * @param searchid
  * @param isclick
  * @param ideaid
  * @param click_timestamp
  * @param click_ip
  * @param ext
  */
case class ParsedClickLog(
                           var searchid: String = "",
                           var isclick: Int = 0,
                           var ideaid: Int = 0,
                           var click_timestamp: Int = 0,
                           var antispam_score: Int = 0,
                           var antispam_rules: String = "",
                           var click_ip: String = "",
                           var ext: collection.Map[String, ExtValue] = null
                         ) extends CommonLog {
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

}


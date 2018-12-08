package com.cpc.spark.small.tool.streaming.parser

/**
  * click日志
  *
  * @param searchid
  * @param isshow
  * @param ideaid
  * @param show_timestamp
  * @param show_ip
  * @param show_refer
  * @param show_ua
  * @param video_show_time
  * @param charge_type
  */
case class ShowLog(
                          var searchid: String = "",
                          var ideaid: Int = 0,
                          var isshow: Int = 0,
                          var show_timestamp: Int = 0,
                          var show_ip: String = "",
                          var show_network: Int = 0,
                          var show_refer: String="",
                          var show_ua: String="",
                          var video_show_time: Int=0,
                          var charge_type: Int=0
//                          var ext: collection.Map[String, ExtValue] = null
                        )


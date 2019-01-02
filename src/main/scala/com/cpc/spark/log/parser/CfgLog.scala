package com.cpc.spark.log.parser

/**
  * Created by Roy on 2017/5/8.
  */
case class CfgLog(
                   aid: String = "",
                   search_timestamp: Int = 0,
                   log_type: String = "", // req/tpl/hdjump
                   request_url: String = "",
                   resp_body: String = "",
                   redirect_url: String = "",
                   template_conf: String = "",
                   adslot_conf: String = "",
                   date: String = "",
                   hour: String = "",
                   ext: collection.Map[String, ExtValue] = null,
                   ip: String = "",
                   ua: String = "",
                   deviceid:String=""
                 )


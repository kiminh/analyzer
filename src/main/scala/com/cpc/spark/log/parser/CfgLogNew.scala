package com.cpc.spark.log.parser

/**
  * @author fym
  * @version created: 2019-11-20 22:11
  * @desc
  */
case class CfgLogNew (
                   aid: String = "",
                   search_timestamp: Int = 0,
                   log_type: String = "", // req/tpl/hdjump
                   request_url: String = "",
                   resp_body: String = "",
                   redirect_url: String = "",
                   template_conf: String = "",
                   adslot_conf: String = "",
                   day: String = "",
                   hour: String = "",
                   ext: collection.Map[String, ExtValue] = null,
                   ip: String = "",
                   ua: String = "",
                   deviceid:String=""
                 )
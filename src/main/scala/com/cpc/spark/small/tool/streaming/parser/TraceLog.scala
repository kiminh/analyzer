package com.cpc.spark.small.tool.streaming.parser

/**
  * Created by Roy on 2017/5/8.
  */
case class TraceLog(
                   searchid: String = "",
                   search_timestamp: Int = 0,
                   trace_type: String = "",
                   trace_os: String = "",
                   trace_refer: String = "",
                   trace_version: String = "",
                   trace_click_count: Int = 0,
                   device_orientation: Int = 0,
                   client_w: Float = 0,
                   client_h: Float = 0,
                   screen_w: Float = 0,
                   screen_h: Float = 0,
                   client_x: Float = 0,
                   client_y: Float = 0,
                   page_x: Float = 0,
                   page_y: Float = 0,
                   trace_ttl: Int = 0,
                   scroll_top: Float = 0,
                   trace_op1: String = "",
                   trace_op2: String = "",
                   trace_op3: String = "",
                   duration: Int = 0,
                   date: String = "",
                   hour: String = "",
                   auto: Int = 0,
                   ip: String = "",
                   adslot_id:Int = 0,
                   ua:String = "",
                   opt: collection.Map[String, String] = null
                   ) {

}


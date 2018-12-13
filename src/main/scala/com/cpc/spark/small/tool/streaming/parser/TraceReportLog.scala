package com.cpc.spark.small.tool.streaming.parser

/**
  * Created by Roy on 2017/5/8.
  */
case class TraceReportLog(
                           searchid:String = "",
                           user_id: Int = 0,
                           plan_id: Int = 0,
                           unit_id: Int = 0,
                           idea_id: Int = 0,
                           date: String = "",
                           hour: String = "",
                           trace_type: String = "",
                           duration: Int = 0,
                           auto: Int = 0
                   ) {
}

